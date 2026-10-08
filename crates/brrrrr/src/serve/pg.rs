//! The PostgreSQL wire protocol (`pgwire`): psql, Grafana, BI tools and drivers (psycopg, JDBC,
//! node-postgres, ADBC's PostgreSQL driver with `use_copy=false`) query `brrrrr serve` as a
//! PostgreSQL server. brrrrr's SQL, not PostgreSQL's: what a client sends to set itself up
//! (`SET`, `BEGIN`, `SHOW`, `version()`) is answered, the rest runs as brrrrr runs it.
//!
//! A column's type is its values' (`write::types`), so a Describe runs the statement: a
//! Describe of a portal keeps its answer for the Execute after it, and a Describe of a statement
//! keeps the types it said, which its Executes then send whatever their rows hold.
use super::{Server, Stop};
use async_trait::async_trait;
use brrrrr_core::value::Value;
use brrrrr_lake::{write, Answer};
use futures::stream;
use pgwire::api::auth::cleartext::CleartextPasswordAuthStartupHandler;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password, StartupHandler};
use pgwire::api::cancel::{CancelHandler, DefaultCancelHandler};
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{
    DataRowEncoder, DescribePortalResponse, DescribeResponse, DescribeStatementResponse, FieldFormat, FieldInfo,
    QueryResponse, Response, Tag,
};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::ConnectionManager;
use pgwire::api::{ClientInfo, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::types::format::FormatOptions;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug)]
struct Token(String);

#[async_trait]
impl AuthSource for Token {
    async fn get_password(&self, _: &LoginInfo) -> PgWireResult<Password> {
        Ok(Password::new(None, self.0.as_bytes().to_vec()))
    }
}

struct Handler {
    server: Arc<Server>,
    parser: Arc<NoopQueryParser>,
    /// Per connection and portal, the bound statement and its answer, run by a Describe of the
    /// portal for the Execute after it.
    answers: Mutex<HashMap<Key, (String, Answer)>>,
    /// Per connection and statement, its SQL and the column types a Describe of it said.
    // ponytail: a connection's entries outlive it, a few per statement it described
    described: Mutex<HashMap<Key, (String, Vec<Type>)>>,
    /// The sessions, by the key a CancelRequest names.
    connections: Arc<ConnectionManager>,
}

/// A connection's portal or statement, by name.
type Key = (SocketAddr, String);

impl NoopStartupHandler for Handler {
    fn connection_manager(&self) -> Option<Arc<ConnectionManager>> {
        Some(self.connections.clone())
    }
}

/// A session's `SET statement_timeout`: `None` for the server's.
struct StatementTimeout(Option<Duration>);

/// `SET [SESSION] statement_timeout = 5000 | '5s' | '1min' | 0 | DEFAULT`, PostgreSQL's (a number
/// is milliseconds; 0 and DEFAULT leave the server's): `None` for another statement.
fn statement_timeout(sql: &str) -> Option<Option<Duration>> {
    let s = sql.trim().trim_end_matches(';').to_ascii_lowercase();
    let s = s.strip_prefix("set")?.trim_start();
    let s = s.strip_prefix("session").unwrap_or(s).trim_start().strip_prefix("statement_timeout")?.trim_start();
    let v = s.strip_prefix('=').or(s.strip_prefix("to"))?.trim().trim_matches(['\'', '"']);
    Some(match v.parse::<u64>() {
        Ok(0) => None,
        Ok(ms) => Some(Duration::from_millis(ms)),
        Err(_) if v == "default" => None,
        Err(_) => {
            let v = v.replace("min", "m");
            brrrrr_core::value::duration_us(&v).map(|us| Duration::from_micros(us as u64))
        }
    })
}

fn error(msg: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new("ERROR".into(), "XX000".into(), msg)))
}

/// What a client sends to set itself up, answered as PostgreSQL would: `None` for a statement
/// brrrrr runs.
fn session(sql: &str) -> Option<Response> {
    let s = sql.trim().trim_end_matches(';').trim().to_ascii_lowercase();
    let one = |name: &str, v: &str| {
        let f = Arc::new(vec![FieldInfo::new(name.into(), None, None, Type::VARCHAR, FieldFormat::Text)]);
        let mut e = DataRowEncoder::new(f.clone());
        let _ = e.encode_field(&Some(v.to_string()));
        Response::Query(QueryResponse::new(f, stream::iter(vec![Ok(e.take_row())])))
    };
    let tag = |t: &str| Some(Response::Execution(Tag::new(t)));
    let first = s.split_whitespace().next().unwrap_or("");
    match first {
        "" => Some(Response::EmptyQuery),
        "set" | "reset" => tag("SET"),
        "begin" | "start" => tag("BEGIN"),
        "commit" | "end" => tag("COMMIT"),
        "rollback" | "abort" => tag("ROLLBACK"),
        "discard" | "deallocate" | "close" | "listen" | "unlisten" => tag(&first.to_ascii_uppercase()),
        "show" if !s.starts_with("show tables") => {
            let v = match s.trim_start_matches("show").trim() {
                "transaction isolation level" => "read committed",
                "server_version" => "16.0",
                "standard_conforming_strings" => "on",
                "client_encoding" | "server_encoding" => "UTF8",
                "timezone" | "time zone" => "UTC",
                "datestyle" => "ISO, MDY",
                _ => "",
            };
            Some(one(s.trim_start_matches("show").trim(), v))
        }
        _ if s == "select version()" => {
            Some(one("version", &format!("PostgreSQL 16.0 (brrrrr {})", env!("CARGO_PKG_VERSION"))))
        }
        _ if s == "select current_schema()" || s == "select current_schema" => Some(one("current_schema", "public")),
        _ if s == "select current_database()" => Some(one("current_database", "brrrrr")),
        _ if s == "select current_user" || s == "select user" => Some(one("current_user", "brrrrr")),
        _ => None,
    }
}

fn pg_type(t: &str) -> Type {
    match t {
        "int" | "uint" => Type::INT8,
        "float" => Type::FLOAT8,
        "bool" => Type::BOOL,
        "time" => Type::TIMESTAMPTZ,
        _ => Type::VARCHAR,
    }
}

/// The columns of `a`, of the types a Describe said (`types`) or else its values'.
fn fields(a: &Answer, format: &Format, types: Option<&[Type]>) -> Vec<FieldInfo> {
    let types: Vec<Type> = match types {
        Some(t) if t.len() == a.columns.len() => t.to_vec(),
        _ => write::types(&a.columns, &a.rows).into_iter().map(pg_type).collect(),
    };
    a.columns
        .iter()
        .zip(types)
        .enumerate()
        .map(|(i, (c, t))| FieldInfo::new(c.clone(), None, None, t, format.format_for(i)))
        .collect()
}

fn response(a: Answer, format: &Format, types: Option<&[Type]>) -> PgWireResult<Response> {
    if a.columns.is_empty() {
        return Ok(Response::Execution(Tag::new(if a.message.is_some() { "OK" } else { "SELECT" })));
    }
    let f = Arc::new(fields(&a, format, types));
    let opts = FormatOptions::default();
    let mut rows = Vec::with_capacity(a.rows.len());
    for r in &a.rows {
        let mut e = DataRowEncoder::new(f.clone());
        // each value as its column's type, whatever it is: a client decodes it as that
        for (v, field) in r.iter().zip(f.iter()) {
            let (ty, fmt) = (field.datatype(), field.format());
            let v = if v.is_null() { None } else { Some(v) };
            match *ty {
                Type::INT8 => e.encode_field_with_type_and_format(&v.and_then(Value::i64), ty, fmt, &opts)?,
                Type::FLOAT8 => e.encode_field_with_type_and_format(&v.and_then(Value::f64), ty, fmt, &opts)?,
                Type::BOOL => {
                    let b = v.and_then(|v| match v {
                        Value::Bool(b) => Some(*b),
                        v => v.f64().map(|f| f != 0.0),
                    });
                    e.encode_field_with_type_and_format(&b, ty, fmt, &opts)?
                }
                Type::TIMESTAMPTZ => {
                    let t = match v {
                        Some(Value::Time(us)) => chrono::DateTime::<chrono::Utc>::from_timestamp_micros(*us),
                        _ => None,
                    };
                    e.encode_field_with_type_and_format(&t, ty, fmt, &opts)?
                }
                _ => {
                    let text = v.map(brrrrr_core::query::text);
                    e.encode_field_with_type_and_format(&text, ty, fmt, &opts)?
                }
            }
        }
        rows.push(Ok(e.take_row()));
    }
    Ok(Response::Query(QueryResponse::new(f, stream::iter(rows))))
}

impl Handler {
    /// Runs `sql` within the session's time limit. A CancelRequest for the session drops this
    /// future (pgwire answers it), which stops the statement.
    async fn run<C: ClientInfo>(&self, client: &C, sql: String) -> PgWireResult<Answer> {
        struct StopOnDrop(Arc<Stop>, bool);
        impl Drop for StopOnDrop {
            fn drop(&mut self) {
                if !self.1 {
                    self.0.stop(super::CANCELED);
                }
            }
        }
        let limit = client.session_extensions().get::<StatementTimeout>().and_then(|t| t.0);
        let stop = Stop::with_limit(limit);
        let mut running = StopOnDrop(stop.clone(), false);
        let s = self.server.clone();
        let done = tokio::task::spawn_blocking(move || s.execute(&sql, &stop)).await;
        running.1 = true;
        done.map_err(|e| error(e.to_string()))?.map_err(|e| error(brrrrr_lake::message(&e)))
    }

    /// A session's statement brrrrr answers itself, its time limit kept.
    fn session<C: ClientInfo>(&self, client: &C, sql: &str) -> Option<Response> {
        if let Some(t) = statement_timeout(sql) {
            client.session_extensions().insert(StatementTimeout(t));
        }
        session(sql)
    }
}

#[async_trait]
impl SimpleQueryHandler for Handler {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let mut out = vec![];
        for s in crate::shell::statements(query) {
            match self.session(client, &s) {
                Some(r) => out.push(r),
                None => out.push(response(self.run(client, s).await?, &Format::UnifiedText, None)?),
            }
        }
        if out.is_empty() {
            out.push(Response::EmptyQuery);
        }
        Ok(out)
    }
}

/// How many parameters `sql` takes: its highest `$n`.
fn parameters(sql: &str) -> usize {
    sql.match_indices('$')
        .filter_map(|(i, _)| {
            let digits = sql[i + 1..].bytes().take_while(u8::is_ascii_digit).count();
            sql[i + 1..i + 1 + digits].parse::<usize>().ok()
        })
        .max()
        .unwrap_or(0)
}

/// The statement with its `$n` parameters as literals, each read as the type the client gave it
/// (in text or binary), a time as text brrrrr reads (`'2024-01-02 09:30:00+00'`); `None` binds
/// every parameter to NULL. A parameter of no type is a number if it reads as one, else text.
fn bind(sql: &str, portal: Option<&Portal<String>>) -> PgWireResult<String> {
    let mut out = sql.to_string();
    let n = portal.map_or(parameters(sql), Portal::parameter_len);
    for i in (0..n).rev() {
        let lit = match portal {
            Some(p) => literal(p, i)?,
            None => "NULL".into(),
        };
        out = out.replace(&format!("${}", i + 1), &lit);
    }
    Ok(out)
}

fn literal(p: &Portal<String>, i: usize) -> PgWireResult<String> {
    let quote = |s: &str| format!("'{}'", s.replace('\'', "''"));
    let Some(Some(raw)) = p.parameters.get(i) else { return Ok("NULL".into()) };
    let ty = p.statement.parameter_types.get(i).cloned().flatten().unwrap_or(Type::UNKNOWN);
    let number =
        matches!(ty, Type::INT2 | Type::INT4 | Type::INT8 | Type::FLOAT4 | Type::FLOAT8 | Type::NUMERIC | Type::OID);
    if !p.parameter_format.is_binary(i) {
        let text = std::str::from_utf8(raw).map_err(|e| error(format!("parameter ${}: {e}", i + 1)))?;
        return Ok(match ty {
            _ if (number || ty == Type::UNKNOWN) && text.trim().parse::<f64>().is_ok() => text.trim().to_string(),
            Type::BOOL => matches!(text.trim(), "t" | "true" | "1" | "on" | "yes" | "y").to_string(),
            _ => quote(text),
        });
    }
    let text = |t: Option<String>| t.unwrap_or_default();
    Ok(match ty {
        Type::INT2 => text(p.parameter::<i16>(i, &ty)?.map(|v| v.to_string())),
        Type::INT4 => text(p.parameter::<i32>(i, &ty)?.map(|v| v.to_string())),
        Type::INT8 => text(p.parameter::<i64>(i, &ty)?.map(|v| v.to_string())),
        Type::FLOAT4 => text(p.parameter::<f32>(i, &ty)?.map(|v| v.to_string())),
        Type::FLOAT8 => text(p.parameter::<f64>(i, &ty)?.map(|v| v.to_string())),
        Type::BOOL => text(p.parameter::<bool>(i, &ty)?.map(|v| v.to_string())),
        Type::TIMESTAMP => quote(&text(
            p.parameter::<chrono::NaiveDateTime>(i, &ty)?.map(|t| t.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        )),
        Type::TIMESTAMPTZ => quote(&text(
            p.parameter::<chrono::DateTime<chrono::FixedOffset>>(i, &ty)?
                .map(|t| t.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
        )),
        Type::DATE => quote(&text(p.parameter::<chrono::NaiveDate>(i, &ty)?.map(|d| d.to_string()))),
        Type::TEXT | Type::VARCHAR | Type::BPCHAR | Type::NAME => quote(&text(p.parameter::<String>(i, &ty)?)),
        t => return Err(error(format!("parameter ${}: {t} in binary is not supported; send it as text", i + 1))),
    })
}

#[async_trait]
impl ExtendedQueryHandler for Handler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = bind(&portal.statement.statement, Some(portal))?;
        if let Some(r) = self.session(client, &sql) {
            return Ok(r);
        }
        let addr = client.socket_addr();
        let kept = self.answers.lock().unwrap().remove(&(addr, portal.name.clone()));
        let a = match kept {
            Some((s, a)) if s == sql => a,
            _ => self.run(client, sql).await?,
        };
        let described = self.described.lock().unwrap().get(&(addr, portal.statement.id.clone())).cloned();
        let types = described.filter(|(s, _)| *s == portal.statement.statement).map(|(_, t)| t);
        response(a, &portal.result_column_format, types.as_deref())
    }

    async fn do_describe_statement<C>(
        &self,
        client: &mut C,
        stmt: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        // a parameter whose type the client left to the server is text, which every client sends
        let n = parameters(&stmt.statement).max(stmt.parameter_types.len());
        let params = (0..n).map(|i| stmt.parameter_types.get(i).cloned().flatten().unwrap_or(Type::TEXT)).collect();
        if session(&stmt.statement).is_some() {
            return Ok(DescribeStatementResponse::new(params, vec![]));
        }
        // its columns, with every parameter NULL: of their values' types, text where it has no row
        let a = self.run(client, bind(&stmt.statement, None)?).await?;
        let f = fields(&a, &Format::UnifiedBinary, None);
        let types = f.iter().map(|f| f.datatype().clone()).collect();
        self.described.lock().unwrap().insert((client.socket_addr(), stmt.id.clone()), (stmt.statement.clone(), types));
        Ok(DescribeStatementResponse::new(params, f))
    }

    async fn do_describe_portal<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        let sql = bind(&portal.statement.statement, Some(portal))?;
        if session(&sql).is_some() {
            return Ok(DescribePortalResponse::no_data());
        }
        let a = self.run(client, sql.clone()).await?;
        if a.columns.is_empty() {
            return Ok(DescribePortalResponse::no_data());
        }
        let addr = client.socket_addr();
        let described = self.described.lock().unwrap().get(&(addr, portal.statement.id.clone())).cloned();
        let types = described.filter(|(s, _)| *s == portal.statement.statement).map(|(_, t)| t);
        let f = fields(&a, &portal.result_column_format, types.as_deref());
        self.answers.lock().unwrap().insert((addr, portal.name.clone()), (sql, a));
        Ok(DescribePortalResponse::new(f))
    }
}

struct Factory {
    handler: Arc<Handler>,
    token: Option<String>,
}

impl PgWireServerHandlers for Factory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.handler.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let mut params = DefaultServerParameterProvider::default();
        params.server_version = "16.0".into();
        let connections = self.handler.connections.clone();
        Arc::new(Startup {
            password: self.token.clone().map(|t| {
                CleartextPasswordAuthStartupHandler::new(Token(t), params).with_connection_manager(connections)
            }),
            handler: self.handler.clone(),
        })
    }

    fn cancel_handler(&self) -> Arc<impl CancelHandler> {
        Arc::new(DefaultCancelHandler::new(self.handler.connections.clone()))
    }
}

/// The token as the password when the server has one; no password otherwise.
struct Startup {
    password: Option<CleartextPasswordAuthStartupHandler<Token, DefaultServerParameterProvider>>,
    handler: Arc<Handler>,
}

#[async_trait]
impl StartupHandler for Startup {
    async fn on_startup<C>(&self, client: &mut C, message: pgwire::messages::PgWireFrontendMessage) -> PgWireResult<()>
    where
        C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: std::fmt::Debug,
        PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
    {
        match &self.password {
            Some(p) => p.on_startup(client, message).await,
            None => self.handler.on_startup(client, message).await,
        }
    }
}

pub fn serve(addr: &str, server: Arc<Server>) -> anyhow::Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().build()?;
    let token = server.token.clone();
    let handler = Handler {
        server,
        parser: Arc::new(NoopQueryParser),
        answers: Default::default(),
        described: Default::default(),
        connections: Default::default(),
    };
    let factory = Arc::new(Factory { handler: Arc::new(handler), token });
    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(addr).await?;
        loop {
            let (socket, _) = listener.accept().await?;
            let f = factory.clone();
            tokio::spawn(async move { pgwire::tokio::process_socket(socket, None, f).await });
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sessions_statement_timeout_is_read_as_postgresql_reads_it() {
        let ms = |n| Some(Some(Duration::from_millis(n)));
        assert_eq!(statement_timeout("SET statement_timeout = 5000"), ms(5000));
        assert_eq!(statement_timeout("set statement_timeout to '5s';"), ms(5000));
        assert_eq!(statement_timeout("SET SESSION statement_timeout = '1min'"), ms(60_000));
        assert_eq!(statement_timeout("SET statement_timeout = '250ms'"), ms(250));
        assert_eq!(statement_timeout("SET statement_timeout = 0"), Some(None));
        assert_eq!(statement_timeout("SET statement_timeout TO DEFAULT"), Some(None));
        assert_eq!(statement_timeout("SET application_name = 'x'"), None);
        assert_eq!(statement_timeout("SELECT 1"), None);
    }
}

//! POC 2: the query with its comments dropped and its own names (CTEs, aliases) made
//! meaningless: `_1`, `_2`, ... The names a table has (`--keep`) stay. The mapping goes to
//! stderr, for the client to read the result's columns back by.
//!
//! obfuscate --keep ts,symbol,price,size < q.sql > q.obf.sql
use anyhow::Result;
use sqlparser::dialect::GenericDialect;
use sqlparser::keywords::Keyword;
use sqlparser::tokenizer::{Token, Tokenizer, Whitespace};
use std::collections::BTreeMap;
use std::io::Read;

fn main() -> Result<()> {
    let keep = sealed::flag(&sealed::args(), "--keep")?;
    let keep: Vec<&str> = keep.split(',').collect();
    let mut sql = String::new();
    std::io::stdin().read_to_string(&mut sql)?;
    let tokens = Tokenizer::new(&GenericDialect {}, &sql).tokenize().map_err(|e| anyhow::anyhow!("{e}"))?;
    let words: Vec<(usize, &Token)> =
        tokens.iter().enumerate().filter(|t| !matches!(t.1, Token::Whitespace(_))).collect();
    // the names a query makes: the word after AS (an alias), the word before AS ( (a CTE)
    let mut names = BTreeMap::new();
    for (i, (_, t)) in words.iter().enumerate() {
        let Token::Word(w) = t else { continue };
        let after_as = i > 0 && matches!(words[i - 1].1, Token::Word(a) if a.keyword == Keyword::AS);
        let before_as = matches!(words.get(i + 1), Some((_, Token::Word(a))) if a.keyword == Keyword::AS)
            && matches!(words.get(i + 2), Some((_, Token::LParen)));
        if (after_as || before_as) && w.keyword == Keyword::NoKeyword && !keep.contains(&w.value.as_str()) {
            let n = names.len() + 1;
            names.entry(w.value.clone()).or_insert(format!("_{n}"));
        }
    }
    let mut out = String::new();
    for t in &tokens {
        match t {
            Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_)) => out.push(' '),
            Token::Word(w) if names.contains_key(&w.value) => out += &names[&w.value],
            t => out += &t.to_string(),
        }
    }
    println!("{}", out.split_whitespace().collect::<Vec<_>>().join(" "));
    for (name, to) in names {
        eprintln!("{to} = {name}");
    }
    Ok(())
}

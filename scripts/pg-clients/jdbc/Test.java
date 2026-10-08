import java.sql.*;
import java.util.*;

public class Test {
    interface Body { Object run() throws Exception; }
    static void check(String name, Body b) {
        try { String s = String.valueOf(b.run()); System.out.println("ok | " + name + " | " + (s.length() > 200 ? s.substring(0, 200) : s)); }
        catch (Exception e) { System.out.println("FAIL | " + name + " | " + e.getClass().getSimpleName() + ": " + e.getMessage()); }
    }
    static List<List<Object>> rows(ResultSet rs) throws SQLException {
        List<List<Object>> out = new ArrayList<>();
        int n = rs.getMetaData().getColumnCount();
        while (rs.next()) { List<Object> r = new ArrayList<>(); for (int i = 1; i <= n; i++) { Object o = rs.getObject(i); r.add(o == null ? null : o.getClass().getSimpleName() + ":" + o); } out.add(r); }
        return out;
    }
    public static void main(String[] a) throws Exception {
        Properties p = new Properties();
        p.setProperty("user", "brrrrr");
        if (a.length > 0) p.setProperty("password", a[0]);
        Connection[] c = new Connection[1];
        check("connect", () -> { c[0] = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:5433/brrrrr", p); return c[0].getMetaData().getDatabaseProductVersion(); });
        Connection conn = c[0];
        check("statement", () -> rows(conn.createStatement().executeQuery("SELECT * FROM bars ORDER BY minute, symbol LIMIT 2")));
        check("metadata types", () -> { ResultSetMetaData m = conn.createStatement().executeQuery("SELECT minute, symbol, open, volume FROM bars LIMIT 1").getMetaData(); List<String> t = new ArrayList<>(); for (int i = 1; i <= 4; i++) t.add(m.getColumnTypeName(i)); return t; });
        check("param text", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE symbol = ?"); s.setString(1, "BTC"); return rows(s.executeQuery()); });
        check("param int", () -> { PreparedStatement s = conn.prepareStatement("SELECT symbol, price FROM trades WHERE size > ? LIMIT 2"); s.setInt(1, 0); return rows(s.executeQuery()); });
        check("param long", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE size > ?"); s.setLong(1, 0L); return rows(s.executeQuery()); });
        check("param double", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE price > ?"); s.setDouble(1, 101.5); return rows(s.executeQuery()); });
        check("param timestamp", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE ts >= ?"); s.setTimestamp(1, Timestamp.from(java.time.Instant.parse("2024-01-02T09:35:00Z"))); return rows(s.executeQuery()); });
        check("param offsetdatetime", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE ts >= ?"); s.setObject(1, java.time.OffsetDateTime.parse("2024-01-02T09:35:00Z")); return rows(s.executeQuery()); });
        check("prepared 6 times (server-prepared)", () -> { PreparedStatement s = conn.prepareStatement("SELECT count(*) FROM trades WHERE symbol = ?"); Object r = null; for (int i = 0; i < 6; i++) { s.setString(1, "ETH"); r = rows(s.executeQuery()); } return r; });
        check("null", () -> rows(conn.createStatement().executeQuery("SELECT NULL AS n, 1 AS one")));
        check("error", () -> rows(conn.createStatement().executeQuery("SELECT nope FROM trades")));
        check("after error", () -> rows(conn.createStatement().executeQuery("SELECT 1")));
        check("dbmeta tables", () -> rows(conn.getMetaData().getTables(null, null, "%", null)));
    }
}

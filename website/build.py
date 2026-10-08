#!/usr/bin/env python3
"""Builds the website into website/_site: each page of pages/ in layout.html, SQL highlighted,
the cookbook made from fixtures/cookbook/cookbook.sql and its DuckDB-checked answers.

    python3 website/build.py && python3 -m http.server -d website/_site
"""
import csv
import html
import json
import re
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
COOKBOOK = HERE.parent / "fixtures" / "cookbook"
BENCH = HERE.parent / "bench" / "sql" / "results.json"
LABELS = {
    "count": "Count rows",
    "per_symbol": "Aggregate per symbol",
    "ohlcv_1m": "One-minute OHLCV bars",
    "vwap_1h": "Hourly VWAP",
    "filter_buys": "Filter, then count",
    "spread_bps": "Average spread per symbol",
    "asof_trades_quotes": "As-of join, one total",
    "tca_per_symbol": "As-of join, TCA per symbol",
    "rolling_return": "Tick returns with lag()",
}
TEASER = ["ohlcv_1m", "vwap_1h", "tca_per_symbol", "rolling_return", "per_symbol"]
OUT = HERE / "_site"
SITE = "https://aperiodic-io.github.io/monotile/"

KEYWORDS = """select from where group by order having limit offset as on join left right inner outer asof
cross union all distinct and or not in is null like ilike between case when then else end over
partition rows range preceding following current row with create replace view table copy to format
describe explain show tables drop live interval true false desc asc filter match_condition cast""".split()
SQL_TOKEN = re.compile(
    r"(?P<comment>--[^\n]*)|(?P<string>'(?:[^']|'')*')|(?P<number>\b\d+(?:\.\d+)?(?:e-?\d+)?\b)"
    r"|(?P<word>[A-Za-z_][A-Za-z_0-9]*)(?P<call>\s*\()?|(?P<other>[\s\S])"
)


def highlight(sql):
    """SQL as HTML with spans for keywords, functions, strings, numbers and comments."""
    out = []
    for m in SQL_TOKEN.finditer(sql):
        kind = m.lastgroup if m.lastgroup != "call" else "word"
        text = m.group(kind)
        if kind == "word":
            if text.lower() in KEYWORDS:
                out.append(f'<span class="k">{text}</span>')
            elif m.group("call"):
                out.append(f'<span class="f">{text}</span>')
            else:
                out.append(html.escape(text))
            if m.group("call"):
                out.append(html.escape(m.group("call")))
        elif kind == "other":
            out.append(html.escape(text))
        else:
            out.append(f'<span class="{kind[0]}">{html.escape(text)}</span>')
    return "".join(out)


PY_TOKEN = re.compile(
    r'(?P<sql>"""[\s\S]*?""")|(?P<comment>#[^\n]*)|(?P<string>"[^"\n]*"|\'[^\'\n]*\')'
    r"|(?P<word>\b(?:import|from|as|with|for|in|def|return|const|new)\b)|(?P<other>[\s\S])"
)
SH_TOKEN = re.compile(r"(?P<prompt>^\$ )|(?P<comment>(?<=\s)#[^\n]*)|(?P<string>\"[^\"]*\"|'[^'\n]*')|(?P<other>[\s\S])", re.M)


def highlight_other(text, lang):
    """Python (and JS) or shell: strings, comments, keywords; the SQL in a triple-quoted string as SQL."""
    out = []
    for m in (PY_TOKEN if lang in ("py", "js") else SH_TOKEN).finditer(text):
        kind, t = m.lastgroup, m.group(m.lastgroup)
        if kind == "sql":
            out.append(f'<span class="s">"""</span>{highlight(t[3:-3])}<span class="s">"""</span>')
        elif kind == "string" and lang == "sh" and re.search(r"\b(SELECT|FROM|CREATE)\b", t):
            out.append(f'<span class="s">{t[0]}</span>{highlight(t[1:-1])}<span class="s">{t[-1]}</span>')
        elif kind == "other":
            out.append(html.escape(t))
        else:
            out.append(f'<span class="{ {"comment": "c", "string": "s", "word": "k", "prompt": "prompt"}[kind]}">{html.escape(t)}</span>')
    return "".join(out)


def highlight_blocks(page):
    """Every `<pre ...><code class="sql|py|js|sh">` block, highlighted (its text is written unescaped)."""

    def one(m):
        text = html.unescape(m.group(3)).strip()
        body = highlight(text) if m.group(2) == "sql" else highlight_other(text, m.group(2))
        return f'<pre{m.group(1)}><code class="{m.group(2)}">{body}</code></pre>'

    return re.sub(r'<pre([^>]*)><code class="(sql|py|js|sh)">([\s\S]*?)</code></pre>', one, page)


def recipes():
    """The cookbook's recipes, in order: name, section, title, about, query."""
    text = (COOKBOOK / "cookbook.sql").read_text()
    out = []
    for chunk in re.split(r"\n(?=-- name: )", text)[1:]:
        meta = {"about": []}
        query = []
        for line in chunk.splitlines():
            m = re.match(r"-- (name|section|title|about|duckdb):\s?(.*)", line)
            if m and m.group(1) == "about":
                meta["about"].append(m.group(2))
            elif m and m.group(1) != "duckdb":
                meta[m.group(1)] = m.group(2)
            elif not m:
                query.append(line)
        meta["about"] = " ".join(meta["about"])
        meta["query"] = "\n".join(query).strip()
        out.append(meta)
    return out


def inline(text):
    """`code` in recipe prose."""
    return re.sub(r"`([^`]+)`", r"<code>\1</code>", html.escape(text, quote=False))


def answer(name, rows=6):
    """The first rows of a recipe's checked answer, as a table."""
    path = COOKBOOK / "expected" / f"{name}.csv"
    if not path.exists():
        return ""
    with path.open() as f:
        table = list(csv.reader(f))
    head, body = table[0], table[1:]
    th = "".join(f"<th>{html.escape(h)}</th>" for h in head)
    tr = "".join(
        "<tr>" + "".join(f"<td>{html.escape(short(v))}</td>" for v in r) + "</tr>" for r in body[:rows]
    )
    more = f'<p class="more">{len(body) - rows} more rows</p>' if len(body) > rows else ""
    return f'<div class="answer"><table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table></div>{more}'


def short(v):
    """A number to 6 significant decimals, as a reader wants it."""
    if re.fullmatch(r"-?\d+\.\d{7,}(e-?\d+)?", v):
        return f"{float(v):.6g}"
    return v


def cookbook():
    all_ = recipes()
    sections = list(dict.fromkeys(r["section"] for r in all_))
    nav = "".join(
        f'<a href="#{slug(s)}">{html.escape(s)} <span>{sum(r["section"] == s for r in all_)}</span></a>'
        for s in sections
    )
    parts = [f'<nav class="chips" aria-label="Sections">{nav}</nav>']
    for s in sections:
        parts.append(f'<h2 id="{slug(s)}">{html.escape(s)}</h2>')
        for r in (r for r in all_ if r["section"] == s):
            parts.append(
                f'<article class="recipe" id="{r["name"]}"><h3><a href="#{r["name"]}">{html.escape(r["title"])}</a></h3>'
                f'<p>{inline(r["about"])}</p>'
                f'<pre><code class="sql">{highlight(r["query"])}</code></pre>{answer(r["name"])}</article>'
            )
    return "\n".join(parts), len(all_)


def bench():
    """The benchmark as bars (the landing page's few, the benchmarks page's all), a legend and a
    table: an engine per bar, in results.json's order (brrrrr first), the fastest time in bold."""
    data = json.loads(BENCH.read_text())
    engines, queries = data["engines"], data["queries"]
    top = max(max(q["s"].values()) for q in queries)
    tag = lambda e: f'e{[x["id"] for x in engines].index(e)}'

    def bars(qs):
        rows = []
        for q in qs:
            cells = "".join(
                f'<div class="bar {tag(e["id"])}" style="width:{q["s"][e["id"]] / top * 100:.1f}%" title="{html.escape(e["name"])}">{q["s"][e["id"]]:.2f}s</div>'
                for e in engines
                if e["id"] in q["s"]
            )
            rows.append(f'<div class="bench-row"><span>{LABELS.get(q["name"], q["name"])}</span><div class="bars">{cells}</div></div>')
        return "".join(rows)

    def row(q):
        best = min(q["s"].values())
        times = "".join(
            f'<td>{"<strong>" if q["s"][e["id"]] == best else ""}{q["s"][e["id"]]:.2f}{"</strong>" if q["s"][e["id"]] == best else ""}</td>'
            for e in engines
        )
        mem = "".join(f'<td>{q["mib"][e["id"]]:,}</td>' for e in engines)
        return f'<tr><td>{LABELS.get(q["name"], q["name"])}</td>{times}{mem}</tr>'

    short = lambda e: html.escape(e["name"].split(" ")[0])
    head = (
        "<tr><th>Query</th>"
        + "".join(f"<th>{short(e)} s</th>" for e in engines)
        + "".join(f"<th>{short(e)} MiB</th>" for e in engines)
        + "</tr>"
    )
    legend = "".join(f'<span class="{tag(e["id"])}">{html.escape(e["name"])}</span>' for e in engines)
    queries_sql = "\n\n".join(f'-- {LABELS.get(q["name"], q["name"])}\n{q["sql"]};' for q in queries)
    teaser = [q for name in TEASER for q in queries if q["name"] == name]
    return {
        "{{bench}}": bars(teaser),
        "{{bench_all}}": bars(queries),
        "{{bench_legend}}": f'<div class="legend">{legend}</div>',
        "{{bench_head}}": head,
        "{{bench_table}}": "".join(row(q) for q in queries),
        "{{bench_engines}}": ", ".join(html.escape(e["name"]) for e in engines[1:]),
        "{{bench_sql}}": highlight(re.sub(r"'[^']*/(trades|quotes)\.parquet'", r"'\1.parquet'", queries_sql)),
    }


def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")


def build():
    if OUT.exists():
        shutil.rmtree(OUT)
    shutil.copytree(HERE / "assets", OUT / "assets")
    layout = (HERE / "layout.html").read_text()
    book, count = cookbook()
    fills = {"{{cookbook}}": book, "{{recipes}}": str(count), **bench()}
    index = []
    for src in sorted((HERE / "pages").rglob("*.html")):
        rel = src.relative_to(HERE / "pages")
        page = src.read_text()
        head = re.match(r"<!--\s*title:\s*(.*?)\s*\|\s*description:\s*(.*?)\s*-->\n", page)
        if not head:
            sys.exit(f"{rel}: the first line is <!-- title: ... | description: ... -->")
        body = highlight_blocks(page[head.end():])
        for k, v in fills.items():
            body = body.replace(k, v)
        root = "../" * (len(rel.parts) - 1)
        section = rel.parts[0] if len(rel.parts) > 1 else rel.stem
        out = (
            layout.replace("{{title}}", head.group(1))
            .replace("{{description}}", head.group(2))
            .replace("{{body}}", body)
            .replace("{{section}}", section)
            .replace("{{root}}", root)
        )
        dest = OUT / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(out)
        index += sections(rel, head.group(1), body)
    # the search box's index (site.js): each page and section, its heading and some of its text
    (OUT / "search.json").write_text(json.dumps(index, ensure_ascii=False, separators=(",", ":")))
    (OUT / ".nojekyll").write_text("")
    shutil.copy(HERE.parent / "scripts" / "install.sh", OUT / "install.sh")
    urls = "".join(f"<url><loc>{SITE}{p.relative_to(OUT).as_posix()}</loc></url>" for p in sorted(OUT.rglob("*.html")))
    (OUT / "sitemap.xml").write_text(f'<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">{urls}</urlset>')
    (OUT / "robots.txt").write_text(f"Sitemap: {SITE}sitemap.xml\n")
    broken = check_links()
    if broken:
        sys.exit("broken links:\n" + "\n".join(f"  {page}: {href}" for page, href in broken))
    print(f"built {OUT}")


def sections(rel, title, body):
    """A page's entries for the search: the page (its title and first paragraph), then each of its
    headings with an id (h2, h3, a cookbook recipe), with the text up to the next heading."""
    text = lambda h: re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", h))).strip()
    page = rel.as_posix()
    name = re.sub(r"\s*·\s*brrrrr$", "", title)
    marks = list(re.finditer(r'<(h[23])[^>]*\bid="([^"]+)"[^>]*>(.*?)</\1>|<article class="recipe" id="([^"]+)"><h3>(.*?)</h3>', body, re.S))
    first = body[: marks[0].start()] if marks else body
    out = [{"u": page, "p": name, "h": name, "t": text(first)[:600]}]
    for i, m in enumerate(marks):
        end = marks[i + 1].start() if i + 1 < len(marks) else len(body)
        anchor, heading = (m.group(2), m.group(3)) if m.group(1) else (m.group(4), m.group(5))
        out.append({"u": f"{page}#{anchor}", "p": name, "h": text(heading), "t": text(body[m.end():end])[:600]})
    return out


def check_links():
    """Every link from one page of the site to another, and to an id on it, leads somewhere."""
    broken = []
    for page in OUT.rglob("*.html"):
        for href in re.findall(r'(?:href|src)="([^"]+)"', page.read_text()):
            if re.match(r"[a-z]+:", href):
                continue
            path, _, frag = href.partition("#")
            target = (page.parent / path).resolve() if path else page
            if not target.exists() or frag and target.suffix == ".html" and f'id="{frag}"' not in target.read_text():
                broken.append((page.relative_to(OUT), href))
    return broken


if __name__ == "__main__":
    build()

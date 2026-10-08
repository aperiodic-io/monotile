"""Seeds the `query` fuzz target's corpus (fuzz/corpus/query) with the queries people write: the
cookbook's recipes, the SQL acceptance scenarios' and the ad-hoc query tests'.

    python3 fuzz/seed_query.py
"""
import glob
import hashlib
import os
import re

root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
out = os.path.join(root, "fuzz", "corpus", "query")
os.makedirs(out, exist_ok=True)
queries = []
for block in open(os.path.join(root, "fixtures/cookbook/cookbook.sql")).read().split("-- name: ")[1:]:
    lines = block.split("\n")[1:]
    queries.append("\n".join(l for l in lines if not l.startswith("--")))
for f in glob.glob(os.path.join(root, "tests/acceptance/sql/*.feature")):
    for m in re.finditer(r'When I run brrrrr sql:\n\s*"""\n(.*?)\n\s*"""', open(f).read(), re.S):
        queries.append("\n".join(l.strip() for l in m.group(1).split("\n")))
tests = open(os.path.join(root, "crates/brrrrr-core/tests/query.rs")).read()
for m in re.finditer(r'"((?:SELECT|WITH)[^"]*)"', tests):
    queries.append(re.sub(r"\\\n\s*", " ", m.group(1)))
n = 0
for q in queries:
    q = q.strip().rstrip(";")
    if q and len(q) <= 4096:
        open(os.path.join(out, hashlib.sha1(q.encode()).hexdigest()[:16]), "w").write(q)
        n += 1
print(f"{n} queries in {out}")

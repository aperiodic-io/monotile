#!/bin/bash
# a panel's queries through Grafana's API: $1 the SQL, $2 the format (time_series or table)
body=$(python3 -c 'import json,sys; print(json.dumps({"queries":[{"refId":"A","datasource":{"uid":"brrrrr"},"rawSql":sys.argv[1],"format":sys.argv[2],"rawQuery":True,"editorMode":"code"}],"from":"1704187800000","to":"1704188400000"}))' "$1" "${2:-time_series}")
curl -s -u admin:admin -H 'Content-Type: application/json' localhost:3000/api/ds/query -d "$body" | python3 -c '
import json,sys
r=json.load(sys.stdin)
for ref,res in r.get("results",{}).items():
    if "error" in res: print("ERROR:", res["error"])
    for f in res.get("frames",[]):
        m=f["schema"].get("meta",{})
        print("SQL:", m.get("executedQueryString"))
        print("fields:", [(x["name"], x.get("type"), (x.get("labels") or {})) for x in f["schema"]["fields"]])
        v=f["data"]["values"]; print("rows:", len(v[0]) if v else 0, "first:", [c[0] for c in v] if v and v[0] else None)
if "message" in r: print(r)
'

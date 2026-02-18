# Spark Lineage Enterprise Demo (Offline)

"
"Interactive lineage UI + sample Latitude pipeline artifacts + eventlog parsing.

"
"## Quickstart
"
"```bash
"
"python3 -m venv .venv
"
"source .venv/bin/activate
"
"pip install -r requirements.txt
"
"./run.sh
"
"```

"
"Open sample:
"
"- http://localhost:8000 (use SSH tunnel if needed)

"
"## Upload your own event log
"
"Home page -> Upload JSONL -> visualize.

"
"## CLI (no UI)
"
"```bash
"
"python cli_parse_eventlog.py samples/latitude/logs/sample_eventlog_latitude.jsonl --out graph.json
"
"```


## AI Demo (Offline)
Open `/ai` for 20 preloaded enterprise questions. This is a deterministic, offline demo grounded in the parsed lineage + transformation metadata embedded in the sample event log.

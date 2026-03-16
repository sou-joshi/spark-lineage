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


## AI Demo questions (use in internal demo)

1. Show full upstream lineage of ORACLE.MART_LATITUDE_DAILY (depth=3).
2. Which S3 raw file paths contributed to ORACLE.MART_LATITUDE_DAILY?
3. List transformations between ORACLE.ODS_LATITUDE and ORACLE.MART_LATITUDE_DAILY.
4. How is ORACLE.MART_LATITUDE_DAILY.LAT_BAND derived?
5. How is ORACLE.MART_LATITUDE_DAILY.PING_COUNT calculated?
6. Show downstream impact if ORACLE.ODS_LATITUDE schema changes (which tables/columns depend on it).
7. Which columns are derived using UDFs?
8. Which columns are aggregated in the MART layer?
9. What is the transformation used to create ORACLE.STG_LATITUDE.MSISDN_HASH?
10. Show lineage chain from S3 RAW to ORACLE MART for LAT_NUMBER.
11. Explain the full data journey for ORACLE.MART_LATITUDE_DAILY in 5 bullets.
12. Which jobs write to ORACLE.ODS_LATITUDE?
13. Which datasets read from S3 CONFORMED paths?
14. Where does ORACLE.MART_LATITUDE_DAILY.EVENT_DATE come from?
15. Show column-level lineage for ORACLE.MART_LATITUDE_DAILY.LAT_BAND.
16. Show column-level lineage for ORACLE.MART_LATITUDE_DAILY.LON_BUCKET.
17. What joins are used in the STAGING step and on which keys?
18. List derived columns created in ORACLE.STG_LATITUDE and their expressions/UDFs.
19. Which transformations are low-confidence and why?
20. If S3 RAW schema adds a new column, what downstream objects are impacted?


## SAS Extension

This repo now includes a SAS lineage pipeline that produces the same normalized graph output as Spark.

Added modules:
- `app/sas_code_parser.py`
- `app/sas_log_parser.py`
- `app/sas_lineage_builder.py`

Supported SAS scenarios in this starter build:
- `LIBNAME` resolution
- `%LET` macro substitution
- `PROC FCMP` user-defined function detection
- `DATA` steps with `SET`, `MERGE`, assignments, `KEEP`
- `PROC SQL` `CREATE TABLE AS SELECT` with joins, aliases, aggregates
- `PROC SUMMARY/MEANS` output datasets
- `PROC APPEND`, `PROC SORT`, `PROC IMPORT`, `PROC EXPORT`
- SAS log evidence for rows read/written and macro resolution

Sample SAS graph key: `sas_customer`
- default table: `MART.CUSTOMER_LATITUDE_DAILY`
- AI demo: `/ai?g=sas_customer`

# Spark Lineage + Offline LLM MVP (Demo-ready)

This MVP demonstrates:
- Ingest **Spark event logs** (JSON lines) + optional Spark driver/executor logs
- Build **dataset lineage** (tables/files) and best-effort **column lineage**
- Search tables/columns
- Ask questions using an **offline LLM** (llama.cpp via `llama-cpp-python`)
- Evidence-first answers (edges include evidence snippets + confidence)

## Quick start (Ubuntu on AWS EC2)
```bash
sudo apt-get update
sudo apt-get install -y python3 python3-venv python3-pip build-essential
cd spark_lineage_offline_mvp
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
chmod +x run.sh
./run.sh
```

Open:
- http://<EC2_IP>:8000

## Offline model (optional but recommended)
Place a GGUF model file here:
- `data/models/model.gguf`

Or set:
```bash
export MODEL_PATH=/path/to/your/model.gguf
```

If no model is present, the app still works using a safe fallback (non-LLM parsing).

## Demo with included sample data
The repo includes sample Spark artifacts:
- `data/logs/sample_eventlog.jsonl`  (Spark event log JSON lines)
- `data/logs/sample_spark_driver.log` (driver log excerpt)
- `data/code/sample_job.py` (PySpark job example)

Steps:
1. Go to `/` and upload `data/logs/sample_eventlog.jsonl`
2. Go to `/search?q=mart_policy_daily` and open lineage
3. Go to `/chat` and ask:
   - "What feeds default.mart_policy_daily?"
   - "Where does customer_email come from?"
   - "Downstream impact of changing risk_score in default.mart_policy_daily?"

## Notes on accuracy
- Dataset lineage is derived from Spark plan text nodes (event log `sparkPlanInfo`), plus log patterns.
- Column lineage is **best-effort** and marked with lower confidence.

## Deploy as a service (optional)
Use systemd or a reverse proxy. For an internal demo, you can run it directly on port 8000 in a restricted SG.

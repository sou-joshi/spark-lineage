import re
from typing import Dict, Any
from app.store import Store

# Very light parsing from driver logs (optional)
# Detect read/write paths and table mentions for additional dataset edges.
READ_PAT = re.compile(r"(?i)reading\s+(s3://[^\s]+|hdfs://[^\s]+|/[^\s]+)")
WRITE_PAT = re.compile(r"(?i)writing\s+(s3://[^\s]+|hdfs://[^\s]+|/[^\s]+)")
TABLE_PAT = re.compile(r"(?i)\b(table|hive)\b\s*:?\s*(?:`)?([a-z0-9_]+)\.(?:`)?([a-z0-9_]+)")

def ingest_driver_log(path: str, store: Store) -> Dict[str, Any]:
    total = 0
    edges = 0
    last_reads = []

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            total += 1
            line = line.strip()
            if not line:
                continue

            m = READ_PAT.search(line)
            if m:
                p = m.group(1)
                store.upsert_dataset(p, kind="file")
                last_reads.append(p)
                last_reads = last_reads[-5:]
                continue

            m = WRITE_PAT.search(line)
            if m:
                p = m.group(1)
                store.upsert_dataset(p, kind="file")
                # connect last reads -> write
                for r in last_reads:
                    store.add_edge(f"ds:{r}", f"ds:{p}", "dataset", f"[driverlog] {line}", 0.55)
                    edges += 1
                continue

            m = TABLE_PAT.search(line)
            if m:
                ds = f"{m.group(2)}.{m.group(3)}"
                store.upsert_dataset(ds, kind="table")
                # connect last reads -> table as sink
                for r in last_reads:
                    store.add_edge(f"ds:{r}", f"ds:{ds}", "dataset", f"[driverlog] {line}", 0.50)
                    edges += 1

    return {"path": path, "total_lines": total, "edges_added": edges, "note": "Driver-log parser adds optional hints for reads/writes."}

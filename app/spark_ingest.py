import json
import re
from typing import Dict, Any, List, Tuple
from app.store import Store

# Table detection patterns seen in Spark plan simpleString/nodeName (heuristic)
TABLE_PATS = [
    re.compile(r"(?i)hivetablerelation\s+`?([a-z0-9_]+)`?\.`?([a-z0-9_]+)`?"),
    re.compile(r"(?i)\brelation\b.*?`?([a-z0-9_]+)`?\.?`?([a-z0-9_]+)`?"),
    re.compile(r"(?i)\bscan\b.*?\b([a-z0-9_]+)\.([a-z0-9_]+)\b"),
    re.compile(r"(?i)\btable\b\s*:?\s*`?([a-z0-9_]+)`?\.`?([a-z0-9_]+)`?"),
    re.compile(r"(?i)\binsert\s+into\s+`?([a-z0-9_]+)`?\.`?([a-z0-9_]+)`?"),
]
FILE_PAT = re.compile(r"(?i)\bfilescan\b.*?(s3://[^\s,]+|hdfs://[^\s,]+|/[^\s,]+)")

# Columns token list (very rough) - used only for demo / best-effort column lineage
BRACKET_COLS_PAT = re.compile(r"\[([a-zA-Z0-9_,\s]+)\]")


def _walk_plan(node: Dict[str, Any]) -> List[Dict[str, Any]]:
    out = []
    stack = [node]
    while stack:
        n = stack.pop()
        out.append(n)
        for c in (n.get("children") or []):
            if isinstance(c, dict):
                stack.append(c)
    return out


def _dedupe(seq: List[str]) -> List[str]:
    seen = set()
    res = []
    for x in seq:
        if x not in seen:
            seen.add(x)
            res.append(x)
    return res


def _extract_tables_and_files(text: str) -> Tuple[List[str], List[str]]:
    t = text or ""
    tables, files = [], []
    for pat in TABLE_PATS:
        for m in pat.finditer(t):
            db, tb = m.group(1), m.group(2)
            if db and tb:
                tables.append(f"{db}.{tb}")
    for m in FILE_PAT.finditer(t):
        files.append(m.group(1))
    return _dedupe(tables), _dedupe(files)


def _extract_cols(text: str) -> List[str]:
    if not text:
        return []
    m = BRACKET_COLS_PAT.search(text)
    if not m:
        return []
    raw = m.group(1)
    cols = []
    for c in raw.split(","):
        c = c.strip()
        if c and re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", c):
            cols.append(c)
    return _dedupe(cols)[:30]


def ingest_spark_eventlog(path: str, store: Store) -> Dict[str, Any]:
    total = 0
    sql_events = 0
    edges = 0
    cols_added = 0

    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            total += 1
            line = line.strip()
            if not line:
                continue
            try:
                evt = json.loads(line)
            except Exception:
                continue

            payload = None
            event_name = None
            if isinstance(evt.get("SparkListenerSQLExecutionStart"), dict):
                payload = evt["SparkListenerSQLExecutionStart"]
                event_name = "SQLExecutionStart"
            elif isinstance(evt.get("SparkListenerSQLAdaptiveExecutionUpdate"), dict):
                payload = evt["SparkListenerSQLAdaptiveExecutionUpdate"]
                event_name = "SQLAdaptiveExecutionUpdate"
            else:
                continue

            sql_events += 1
            plan = payload.get("sparkPlanInfo")
            desc = payload.get("description") or payload.get("details") or payload.get("sparkPlanDescription") or event_name
            if not isinstance(plan, dict):
                continue

            nodes = _walk_plan(plan)
            plan_texts = []
            all_tables, all_files = [], []

            for n in nodes:
                txt = (n.get("nodeName") or "") + " " + (n.get("simpleString") or "")
                plan_texts.append(txt)
                tbs, fls = _extract_tables_and_files(txt)
                all_tables.extend(tbs)
                all_files.extend(fls)

            all_tables = _dedupe(all_tables)
            all_files = _dedupe(all_files)

            for t in all_tables:
                store.upsert_dataset(t, kind="table")
            for p in all_files:
                store.upsert_dataset(p, kind="file")

            # sink guess: explicit "insert into db.table" in description OR last table in list
            sink = None
            m = re.search(r"(?i)insert\s+into\s+`?([a-z0-9_]+)`?\.`?([a-z0-9_]+)`?", desc or "")
            if m:
                sink = f"{m.group(1)}.{m.group(2)}"
                store.upsert_dataset(sink, kind="table")
            elif all_tables:
                sink = all_tables[-1]

            sources = [x for x in (all_tables + all_files) if x != sink]
            if sink and sources:
                for s in sources:
                    store.add_edge(
                        src=f"ds:{s}",
                        dst=f"ds:{sink}",
                        edge_type="dataset",
                        evidence=f"[{event_name}] {desc}\nPlan: {plan_texts[-1][:400]}",
                        confidence=0.80 if m else 0.60,
                    )
                    edges += 1

            # Column best-effort:
            if sink:
                cols = []
                for txt in plan_texts[:25]:
                    cols.extend(_extract_cols(txt))
                cols = _dedupe(cols)
                for c in cols:
                    store.upsert_column(sink, c); cols_added += 1
                    # weak: connect same-named columns from other source tables to sink
                    for src_tb in all_tables[:-1]:
                        store.upsert_column(src_tb, c); cols_added += 1
                        store.add_edge(
                            src=f"col:{src_tb}.{c}",
                            dst=f"col:{sink}.{c}",
                            edge_type="column",
                            evidence=f"Heuristic token match for column '{c}' in plan text.",
                            confidence=0.35,
                        )
                        edges += 1

    return {
        "path": path,
        "total_lines": total,
        "sql_events_seen": sql_events,
        "edges_added": edges,
        "columns_added": cols_added,
        "note": "Heuristic lineage from Spark plan text (demo MVP). Dataset lineage is usually good; column lineage is best-effort."
    }

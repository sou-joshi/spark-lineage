
from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
import json
import re
from pathlib import Path

DATASET_PREFIX = "ds:"
COL_PREFIX = "col:"

@dataclass
class Node:
    id: str
    type: str  # dataset|column
    name: str
    kind: Optional[str] = None
    dataset: Optional[str] = None
    column: Optional[str] = None

@dataclass
class Edge:
    u: str
    v: str
    edge_type: str  # dataset|column|contains
    confidence: float = 0.6
    evidence: str = ""
    transformation: Optional[Dict[str, Any]] = None

def ds(name: str) -> str:
    return f"{DATASET_PREFIX}{name}"

def col_id(dataset: str, colname: str) -> str:
    return f"{COL_PREFIX}{dataset}.{colname}"

def _guess_kind(dataset_name: str) -> str:
    if dataset_name.startswith("s3://"):
        return "file"
    return "table"

def _walk_plan(node: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    stack = [node]
    while stack:
        n = stack.pop()
        yield n
        for ch in (n.get("children") or []):
            stack.append(ch)

_FILESCAN_RE = re.compile(r"FileScan\s+\w+\s+(?P<path>\S+)\s+\[(?P<cols>.*?)\]")
_JDBC_RE = re.compile(r"JDBCRelation\((?P<table>[^)]+)\)\s+\[(?P<cols>.*?)\]")
_INSERT_TARGET_RE = re.compile(r"InsertIntoHiveTable\s+(?P<target>.+)$")

def _parse_cols(cols_str: str) -> List[str]:
    cols = [c.strip() for c in cols_str.split(",")]
    return [c for c in cols if c]

def extract_from_plan(plan: Dict[str, Any]) -> Tuple[Set[str], Set[str], Dict[str, List[str]], Dict[str, List[str]], Optional[str], str]:
    file_scans: Set[str] = set()
    jdbc_sources: Set[str] = set()
    scan_cols: Dict[str, List[str]] = {}
    jdbc_cols: Dict[str, List[str]] = {}
    insert_target: Optional[str] = None

    evidence_bits: List[str] = []
    for n in _walk_plan(plan):
        s = (n.get("simpleString") or "").strip()
        if s:
            evidence_bits.append(s)

        m = _FILESCAN_RE.search(s)
        if m:
            path = m.group("path")
            cols = _parse_cols(m.group("cols"))
            file_scans.add(path)
            scan_cols[path] = cols

        m = _JDBC_RE.search(s)
        if m:
            table = m.group("table")
            cols = _parse_cols(m.group("cols"))
            jdbc_sources.add(table)
            jdbc_cols[table] = cols

        m = _INSERT_TARGET_RE.search(s)
        if m:
            insert_target = m.group("target").strip()

    raw_evidence = "\n".join(evidence_bits[:40])
    return file_scans, jdbc_sources, scan_cols, jdbc_cols, insert_target, raw_evidence

def parse_eventlog_jsonl(path: Path) -> Dict[str, Any]:
    nodes: Dict[str, Node] = {}
    edges: List[Edge] = []

    def ensure_dataset(name: str):
        nid = ds(name)
        if nid not in nodes:
            nodes[nid] = Node(id=nid, type="dataset", name=name, kind=_guess_kind(name))
        return nid

    def ensure_column(dataset: str, colname: str):
        did = ensure_dataset(dataset)
        cid = col_id(dataset, colname)
        if cid not in nodes:
            nodes[cid] = Node(id=cid, type="column", name=f"{dataset}.{colname}", dataset=dataset, column=colname)
            edges.append(Edge(u=did, v=cid, edge_type="contains", confidence=1.0, evidence="schema/plan"))
        return cid

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            payload = obj.get("SparkListenerSQLExecutionStart") or {}
            plan = payload.get("sparkPlanInfo")
            if not plan:
                continue

            file_scans, jdbc_sources, scan_cols, jdbc_cols, insert_target, _raw_evidence = extract_from_plan(plan)
            sources = list(file_scans) + list(jdbc_sources)
            target = insert_target
            if target:
                target = target.strip("`")

            if target and sources:
                t_id = ensure_dataset(target)
                for s in sources:
                    s_id = ensure_dataset(s)
                    edges.append(Edge(u=s_id, v=t_id, edge_type="dataset", confidence=0.85, evidence=payload.get("description","") or "SQLExecutionStart"))

                for s, cols in scan_cols.items():
                    for c in cols:
                        ensure_column(s, c)
                for s, cols in jdbc_cols.items():
                    for c in cols:
                        ensure_column(s, c)

                tgt_cols: List[str] = []
                if target in scan_cols:
                    tgt_cols = scan_cols[target]
                elif target in jdbc_cols:
                    tgt_cols = jdbc_cols[target]
                else:
                    for s in sources:
                        if s in scan_cols:
                            tgt_cols = scan_cols[s]
                            break
                        if s in jdbc_cols:
                            tgt_cols = jdbc_cols[s]
                            break

                for tc in tgt_cols[:60]:
                    ensure_column(target, tc)


                # --- Enterprise demo: explicit transformation metadata (if present in eventlog) ---
                # Eventlog records may include: transformations: [{target_col, sources:[..], expr, kind, udf, confidence, evidence}]
                for t in (payload.get("transformations") or []):
                    try:
                        tgt_col = t.get("target_col") or t.get("output_column") or t.get("target")
                        if not tgt_col:
                            continue
                        # normalize common keys
                        if "expr" not in t and "expression" in t:
                            t["expr"] = t.get("expression")
                        if "kind" not in t and "type" in t:
                            t["kind"] = t.get("type")
                        if "sources" not in t and "inputs" in t:
                            # inputs may be ["LAT_NUMBER"] (assumed within first source dataset if available)
                            t["sources"] = t.get("inputs")

                        # target_col can be "dataset.col" or just "col" (assumed on target dataset)
                        if "." in tgt_col:
                            tgt_ds, tgt_c = tgt_col.split(".", 1)
                        else:
                            tgt_ds, tgt_c = (target or ""), tgt_col
                        if not tgt_ds:
                            continue
                        tgt_id = ensure_column(tgt_ds, tgt_c)

                        sources_list = t.get("sources") or []
                        if not sources_list and t.get("source"):
                            sources_list = [t.get("source")]
                        if not sources_list:
                            continue

                        # add derived edges from each source column into target column
                        for sc_full in sources_list:
                            if not sc_full:
                                continue
                            if "." in sc_full:
                                sds, sc = sc_full.split(".", 1)
                            else:
                                sds, sc = (sources[0] if sources else ""), sc_full
                            if not sds:
                                continue
                            src_id = ensure_column(sds, sc)
                            edges.append(Edge(
                                u=src_id,
                                v=tgt_id,
                                edge_type="column",
                                confidence=float(t.get("confidence") or 0.9),
                                evidence=str(t.get("evidence") or t.get("expr") or "transformation metadata"),
                                transformation={
                                    "kind": t.get("kind") or "expr",
                                    "udf": t.get("udf"),
                                    "expr": t.get("expr"),
                                    "sources": sources_list,
                                    "target_col": f"{tgt_ds}.{tgt_c}",
                                    "confidence": float(t.get("confidence") or 0.9),
                                }
                            ))
                    except Exception:
                        continue

                def norm(x: str) -> str:
                    return re.sub(r"[^a-z0-9]+","", x.lower())

                tgt_norm = {norm(tc): tc for tc in tgt_cols}
                for s in sources:
                    scol_list = scan_cols.get(s) or jdbc_cols.get(s) or []
                    for sc in scol_list[:80]:
                        nsc = norm(sc)
                        match = tgt_norm.get(nsc)
                        if match:
                            edges.append(Edge(
                                u=ensure_column(s, sc),
                                v=ensure_column(target, match),
                                edge_type="column",
                                confidence=0.62,
                                evidence="heuristic name match from plan"
                            ))
            else:
                for s in sources:
                    ensure_dataset(s)

    return {
        "nodes": [vars(n) for n in nodes.values()],
        "edges": [vars(e) for e in edges],
    }


from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, List
from app.sas_code_parser import parse_sas_code
from app.sas_log_parser import parse_sas_log


def _guess_kind(name: str) -> str:
    u = (name or '').upper()
    if u.startswith('S3://') or '/' in name or name.lower().endswith(('.csv','.txt','.dat')):
        return 'file'
    return 'table'


def ds(name: str) -> str:
    return f'ds:{name}'


def col_id(dataset: str, col: str) -> str:
    return f'col:{dataset}.{col}'


def build_sas_graph(code_path: Path, log_path: Path | None = None) -> Dict[str, Any]:
    log_info = parse_sas_log(log_path) if log_path else {"reads": [], "writes": [], "macro_lines": [], "symbols": {}, "warnings": []}
    code_info = parse_sas_code(code_path, extra_macros=log_info.get('symbols') or {})

    nodes: Dict[str, Dict[str, Any]] = {}
    edges: List[Dict[str, Any]] = []

    def ensure_dataset(name: str):
        nid = ds(name)
        if nid not in nodes:
            nodes[nid] = {"id": nid, "type": "dataset", "name": name, "kind": _guess_kind(name)}
        return nid

    def ensure_col(dataset: str, col: str):
        did = ensure_dataset(dataset)
        cid = col_id(dataset, col)
        if cid not in nodes:
            nodes[cid] = {"id": cid, "type": "column", "name": f"{dataset}.{col}", "dataset": dataset, "column": col}
            edges.append({"u": did, "v": cid, "edge_type": "contains", "confidence": 1.0, "evidence": "code/log schema hint"})
        return cid

    # materialize code-derived lineage
    for step in code_info['steps']:
        target = step['target']
        ensure_dataset(target)
        for s in step.get('sources', []):
            ensure_dataset(s)
            edges.append({"u": ds(s), "v": ds(target), "edge_type": "dataset", "confidence": 0.86, "evidence": step['evidence'], "transformation": {"kind": step['type']}})
        for t in step.get('transformations', []):
            tgt_col = t.get('target_col')
            if not tgt_col:
                continue
            tgt_id = ensure_col(target, tgt_col)
            src_cols = t.get('sources') or []
            if not src_cols and step.get('sources'):
                # still create target containment so it shows in UI
                continue
            for sc in src_cols:
                # if source column not fully qualified, attach to first source dataset or all if join-ambiguous
                src_datasets = step.get('sources') or [target]
                chosen = src_datasets[0]
                src_id = ensure_col(chosen, sc)
                edges.append({
                    "u": src_id,
                    "v": tgt_id,
                    "edge_type": "column",
                    "confidence": 0.84 if t.get('kind') in ('aggregate','udf','expr') else 0.72,
                    "evidence": t.get('evidence') or step['evidence'],
                    "transformation": {
                        "kind": t.get('kind') or step['type'],
                        "expr": t.get('expr'),
                        "udf": t.get('udf'),
                        "sources": [f"{chosen}.{sc}"],
                        "target_col": f"{target}.{tgt_col}",
                        "confidence": 0.84,
                    }
                })

    # attach row count evidence from logs to dataset edges/nodes via extra pseudo edges? keep as dataset evidence edges if missing
    log_reads = {r['dataset']: r for r in log_info.get('reads', [])}
    log_writes = {w['dataset']: w for w in log_info.get('writes', [])}
    for step in code_info['steps']:
        target = step['target']
        for s in step.get('sources', []):
            r = log_reads.get(s)
            w = log_writes.get(target)
            if r or w:
                edges.append({
                    "u": ds(s),
                    "v": ds(target),
                    "edge_type": "dataset",
                    "confidence": 0.95,
                    "evidence": ' | '.join(x['text'] for x in [r,w] if x),
                    "transformation": {"kind": f"{step['type']}_log_confirmed"}
                })

    # warnings as metadata node if any
    if log_info.get('warnings'):
        ensure_dataset('SAS.LOG_WARNINGS')

    return {"nodes": list(nodes.values()), "edges": edges, "metadata": {"source_type": "sas", "warnings": log_info.get('warnings', []), "functions": [f['name'] for f in code_info.get('functions', [])]}}

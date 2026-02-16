import os
import json
import re
from typing import Dict, Any, Optional, List
import networkx as nx

from app.store import Store

SYSTEM_PROMPT = """You are a data lineage assistant.
Return STRICT JSON only (no markdown) with this schema:

{
  "intent": "dataset_lineage" | "column_lineage" | "impact" | "search",
  "target": "<string>",
  "direction": "up" | "down" | "both",
  "depth": 1 | 2 | 3
}

Rules:
- "where does X come from" => direction="up"
- "what feeds X" => direction="up" (X is sink) OR if asked "what does X feed" => direction="down"
- "impact/break" => intent="impact" direction="down"
- If unsure => intent="search"
- depth default 2
"""

def _load_llm(model_path: str):
    if not os.path.exists(model_path):
        return None
    try:
        from llama_cpp import Llama
    except Exception:
        return None
    return Llama(
        model_path=model_path,
        n_ctx=2048,
        n_threads=max(2, os.cpu_count() or 2),
        n_gpu_layers=0,
        verbose=False,
    )

def _fallback_spec(question: str) -> Dict[str, Any]:
    q = question.lower()
    intent = "dataset_lineage"
    direction = "both"
    depth = 2

    # detect column
    if "." in question or "column" in q:
        intent = "column_lineage"

    if "come from" in q or "upstream" in q or "where does" in q:
        direction = "up"
    if "what does" in q and "feed" in q:
        direction = "down"
    if "impact" in q or "break" in q or "downstream" in q:
        intent = "impact"
        direction = "down"

    # target guess: last token (strip punctuation)
    toks = re.findall(r"[A-Za-z0-9_\.]+", question)
    target = toks[-1] if toks else question.strip()

    return {"intent": intent, "target": target, "direction": direction, "depth": depth}

def _collect_evidence(edges: List[Dict[str, Any]], limit: int = 6) -> List[str]:
    evs = []
    for e in edges[:limit]:
        ev = (e.get("evidence") or "").strip().replace("\n", " ")
        if ev:
            evs.append(f"{e.get('u')} -> {e.get('v')} (conf={e.get('confidence')})\n{ev[:240]}")
    return evs

def _query(store: Store, g: nx.DiGraph, spec: Dict[str, Any]) -> Dict[str, Any]:
    intent = spec.get("intent")
    target = (spec.get("target") or "").strip()
    depth = int(spec.get("depth") or 2)
    direction = spec.get("direction") or "both"

    if intent == "search":
        return {"kind": "search", "results": store.search(target)}

    if intent in ("dataset_lineage", "impact"):
        view = store.get_dataset_lineage_view(g, target, depth=depth)
        if view.get("not_found"):
            return {"kind": "not_found", "message": f"Dataset '{target}' not found", "suggest": store.search(target)}
        up = view["up"]; down = view["down"]
        if direction == "up":
            return {"kind": "dataset", "target": target, "direction": "up", "data": up}
        if direction == "down":
            return {"kind": "dataset", "target": target, "direction": "down", "data": down}
        return {"kind": "dataset", "target": target, "direction": "both", "data": {"up": up, "down": down}}

    if intent == "column_lineage":
        view = store.get_column_lineage_view(g, target, depth=depth)
        if view.get("not_found"):
            return {"kind": "not_found", "message": f"Column '{target}' not found", "suggest": store.search(target)}
        up = view["up"]; down = view["down"]
        if direction == "up":
            return {"kind": "column", "target": target, "direction": "up", "data": up}
        if direction == "down":
            return {"kind": "column", "target": target, "direction": "down", "data": down}
        return {"kind": "column", "target": target, "direction": "both", "data": {"up": up, "down": down}}

    return {"kind": "unknown", "spec": spec}

def answer_question(question: str, graph: nx.DiGraph, store: Store, model_path: str) -> Dict[str, Any]:
    llm = _load_llm(model_path)

    if llm is None:
        spec = _fallback_spec(question)
        result = _query(store, graph, spec)
        # evidence for UI
        edges = []
        data = result.get("data") or {}
        if isinstance(data, dict) and "edges" in data:
            edges = data.get("edges", [])
        elif isinstance(data, dict) and "up" in data:
            edges = (data["up"].get("edges", []) + data["down"].get("edges", [])) if "down" in data else data["up"].get("edges", [])
        evidence = _collect_evidence(edges)
        return {
            "spec": spec,
            "summary": f"(No model) Resolved intent={spec['intent']} direction={spec['direction']} target={spec['target']}",
            "evidence": evidence,
            "result": result,
            "model_used": None,
        }

    # 1) intent JSON
    resp = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ],
        temperature=0.1,
    )
    content = (resp["choices"][0]["message"]["content"] or "").strip()
    try:
        spec = json.loads(content)
    except Exception:
        spec = _fallback_spec(question)

    result = _query(store, graph, spec)

    # gather evidence snippets
    edges = []
    data = result.get("data") or {}
    if isinstance(data, dict) and "edges" in data:
        edges = data.get("edges", [])
    elif isinstance(data, dict) and "up" in data:
        edges = (data["up"].get("edges", []) + data["down"].get("edges", [])) if "down" in data else data["up"].get("edges", [])
    evidence = _collect_evidence(edges)

    # 2) concise summary with evidence
    prompt = {
        "question": question,
        "spec": spec,
        "result_kind": result.get("kind"),
        "target": result.get("target"),
        "direction": result.get("direction"),
        "evidence": evidence[:4],
        "instructions": "Answer in 6-10 lines. Give a confidence 0-1. Mention evidence snippets. Admit if heuristic/partial.",
    }
    resp2 = llm.create_chat_completion(
        messages=[
            {"role": "system", "content": "You summarize lineage results for an internal demo. Be practical and concise."},
            {"role": "user", "content": json.dumps(prompt)},
        ],
        temperature=0.2,
    )
    summary = (resp2["choices"][0]["message"]["content"] or "").strip()

    return {"spec": spec, "summary": summary, "evidence": evidence, "result": result, "model_used": os.path.basename(model_path)}

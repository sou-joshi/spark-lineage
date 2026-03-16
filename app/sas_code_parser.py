
from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, List, Any, Tuple

IDENT_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)")
DATASET_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)")
LET_RE = re.compile(r"%let\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*;", re.I)
LIBNAME_RE = re.compile(r"libname\s+([A-Za-z_][A-Za-z0-9_]*)\s+(.*?);", re.I | re.S)
INCLUDE_RE = re.compile(r"%include\s+["']([^"']+)["']\s*;", re.I)
FCMP_FUNC_RE = re.compile(r"proc\s+fcmp;(.*?)run;", re.I | re.S)
FCMP_DEF_RE = re.compile(r"function\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*?)\)\s*;(.+?)endsub;", re.I | re.S)
PROC_SQL_BLOCK_RE = re.compile(r"proc\s+sql\s*;(.+?)quit;", re.I | re.S)
DATA_BLOCK_RE = re.compile(r"data\s+([^;]+);(.+?)run;", re.I | re.S)
PROC_APPEND_RE = re.compile(r"proc\s+append\s+base\s*=\s*([A-Za-z0-9_\.]+)\s+data\s*=\s*([A-Za-z0-9_\.]+).*?;", re.I | re.S)
PROC_SORT_RE = re.compile(r"proc\s+sort\s+data\s*=\s*([A-Za-z0-9_\.]+)(?:\s+out\s*=\s*([A-Za-z0-9_\.]+))?.*?;", re.I | re.S)
PROC_SUMMARY_RE = re.compile(r"proc\s+(summary|means)\s+data\s*=\s*([A-Za-z0-9_\.]+)(.+?)run;", re.I | re.S)
PROC_IMPORT_RE = re.compile(r"proc\s+import\s+datafile\s*=\s*["']([^"']+)["']\s+out\s*=\s*([A-Za-z0-9_\.]+).*?;", re.I | re.S)
PROC_EXPORT_RE = re.compile(r"proc\s+export\s+data\s*=\s*([A-Za-z0-9_\.]+)\s+outfile\s*=\s*["']([^"']+)["'].*?;", re.I | re.S)

STOPWORDS = {"data","set","merge","if","then","else","end","do","run","quit","proc","sql","create","table","select","from","left","right","inner","outer","join","on","where","group","by","as","and","or","not","sum","avg","min","max","count","case","when","format","length","keep","drop","rename","output","class","var"}


def _norm_ds(ds: str) -> str:
    return ds.strip().rstrip('.').upper()


def _apply_macros(text: str, macros: Dict[str, str]) -> str:
    out = text
    for _ in range(5):
        changed = False
        for k, v in macros.items():
            patterns = [f'&{k}', f'&{k}.']
            for p in patterns:
                if p in out:
                    out = out.replace(p, v)
                    changed = True
        if not changed:
            break
    return out


def _extract_expr_sources(expr: str) -> List[str]:
    tokens = []
    for t in IDENT_RE.findall(expr or ''):
        tl = t.lower()
        if tl in STOPWORDS:
            continue
        if tl.isdigit():
            continue
        tokens.append(t.upper())
    # preserve order unique
    seen = set(); out=[]
    for t in tokens:
        if t not in seen:
            out.append(t); seen.add(t)
    return out


def _split_select_list(select_text: str) -> List[str]:
    items=[]; cur=[]; depth=0
    for ch in select_text:
        if ch == '(':
            depth += 1
        elif ch == ')':
            depth = max(0, depth-1)
        if ch == ',' and depth == 0:
            item=''.join(cur).strip()
            if item:
                items.append(item)
            cur=[]
        else:
            cur.append(ch)
    item=''.join(cur).strip()
    if item:
        items.append(item)
    return items


def parse_sas_code(path: Path, extra_macros: Dict[str, str] | None = None) -> Dict[str, Any]:
    text = path.read_text(encoding='utf-8', errors='ignore')
    macros: Dict[str, str] = dict(extra_macros or {})
    for k,v in LET_RE.findall(text):
        macros[k.upper()] = v.strip()
    text = _apply_macros(text, macros)

    libs=[]
    includes=[]
    functions=[]
    steps=[]
    # libnames / includes
    for lib, target in LIBNAME_RE.findall(text):
        libs.append({"lib": lib.upper(), "target": target.strip()})
    for inc in INCLUDE_RE.findall(text):
        includes.append(inc)

    # FCMP functions as SAS UDFs
    for block in FCMP_FUNC_RE.findall(text):
        for name, params, body in FCMP_DEF_RE.findall(block):
            functions.append({"name": name.upper(), "params": [p.strip().upper() for p in params.split(',') if p.strip()], "body": body.strip()})

    # DATA steps
    for target_raw, body in DATA_BLOCK_RE.findall(text):
        targets = [_norm_ds(x) for x in re.split(r"\s+", target_raw.strip()) if '.' in x]
        if not targets:
            continue
        target = targets[0]
        srcs=[]
        for kw in [r"set\s+([^;]+);", r"merge\s+([^;]+);", r"update\s+([^;]+);", r"modify\s+([^;]+);"]:
            for m in re.finditer(kw, body, re.I):
                for ds in DATASET_RE.findall(m.group(1)):
                    srcs.append(_norm_ds('.'.join(ds)))
        transforms=[]
        for line in body.splitlines():
            s=line.strip()
            if not s or s.startswith('*'):
                continue
            m = re.match(r"([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.+?);$", s)
            if m:
                tgt = m.group(1).upper(); expr = m.group(2).strip()
                sources = _extract_expr_sources(expr)
                kind = 'expr'; udf = None
                for fn in functions:
                    if re.search(rf"{re.escape(fn['name'])}\s*\(", expr, re.I):
                        kind='udf'; udf=fn['name']
                        break
                transforms.append({"target_col": tgt, "sources": sources, "expr": expr, "kind": kind, "udf": udf, "evidence": f"DATA step assignment: {s}"})
        steps.append({"type":"data_step", "target": target, "sources": srcs, "transformations": transforms, "evidence": f"DATA {target}"})

    # PROC SQL
    for block in PROC_SQL_BLOCK_RE.findall(text):
        create_m = re.search(r"create\s+table\s+([A-Za-z0-9_\.]+)\s+as\s+select\s+(.+?)\s+from\s+(.+)$", block, re.I | re.S)
        if create_m:
            target = _norm_ds(create_m.group(1))
            select_text = create_m.group(2)
            rest = create_m.group(3)
            srcs=[]
            for m in re.finditer(r"(?:from|join)\s+([A-Za-z0-9_\.]+)", rest, re.I):
                srcs.append(_norm_ds(m.group(1)))
            transforms=[]
            for item in _split_select_list(select_text):
                alias_m = re.search(r"as\s+([A-Za-z_][A-Za-z0-9_]*)$", item, re.I)
                tgt = alias_m.group(1).upper() if alias_m else None
                expr = re.sub(r"as\s+[A-Za-z_][A-Za-z0-9_]*$", "", item, flags=re.I).strip()
                if not tgt:
                    # use final identifier if simple col ref
                    if '.' in expr:
                        tgt = expr.split('.')[-1].upper()
                    else:
                        tgt = re.sub(r"[^A-Za-z0-9_]", "", expr).upper()[:64] or 'COL'
                sources = []
                for ds_alias, col in re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)", expr):
                    sources.append(col.upper())
                if not sources:
                    sources = _extract_expr_sources(expr)
                kind='aggregate' if re.search(r"(sum|avg|min|max|count)\s*\(", expr, re.I) else 'expr'
                transforms.append({"target_col": tgt, "sources": sources, "expr": expr, "kind": kind, "evidence": f"PROC SQL select: {item.strip()}"})
            steps.append({"type":"proc_sql", "target": target, "sources": srcs, "transformations": transforms, "evidence": f"PROC SQL create table {target}"})

    # PROC APPEND / SORT / SUMMARY / IMPORT / EXPORT
    for base, data in PROC_APPEND_RE.findall(text):
        steps.append({"type":"proc_append", "target": _norm_ds(base), "sources": [_norm_ds(data)], "transformations": [], "evidence": f"PROC APPEND base={base} data={data}"})
    for data, out in PROC_SORT_RE.findall(text):
        target = _norm_ds(out or data)
        steps.append({"type":"proc_sort", "target": target, "sources": [_norm_ds(data)], "transformations": [], "evidence": f"PROC SORT data={data} out={out or data}"})
    for procname, data, body in PROC_SUMMARY_RE.findall(text):
        out_m = re.search(r"output\s+out\s*=\s*([A-Za-z0-9_\.]+)\s+(.+?);", body, re.I | re.S)
        if out_m:
            target = _norm_ds(out_m.group(1))
            outspec = out_m.group(2)
            transforms=[]
            for func, tgt in re.findall(r"(sum|avg|min|max|n)\s*=\s*([A-Za-z_][A-Za-z0-9_]*)", outspec, re.I):
                transforms.append({"target_col": tgt.upper(), "sources": [], "expr": f"{func.lower()}(...)", "kind": "aggregate", "evidence": f"PROC {procname.upper()} output {func}={tgt}"})
            steps.append({"type": f"proc_{procname.lower()}", "target": target, "sources": [_norm_ds(data)], "transformations": transforms, "evidence": f"PROC {procname.upper()} data={data}"})
    for file, out in PROC_IMPORT_RE.findall(text):
        steps.append({"type":"proc_import", "target": _norm_ds(out), "sources": [file], "transformations": [], "evidence": f"PROC IMPORT {file} -> {out}"})
    for data, file in PROC_EXPORT_RE.findall(text):
        steps.append({"type":"proc_export", "target": file, "sources": [_norm_ds(data)], "transformations": [], "evidence": f"PROC EXPORT {data} -> {file}"})

    return {"macros": macros, "libs": libs, "includes": includes, "functions": functions, "steps": steps}

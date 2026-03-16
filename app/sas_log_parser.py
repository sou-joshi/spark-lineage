
from __future__ import annotations
import re
from pathlib import Path
from typing import Dict, List, Any

READ_RE = re.compile(r"NOTE:\s+There were\s+(?P<rows>\d+)\s+observations read from the data set\s+(?P<ds>[A-Z0-9_\.]+)\.", re.I)
WRITE_RE = re.compile(r"NOTE:\s+The data set\s+(?P<ds>[A-Z0-9_\.]+)\s+has\s+(?P<rows>\d+)\s+observations\s+and\s+(?P<cols>\d+)\s+variables\.", re.I)
MPRINT_RE = re.compile(r"MPRINT\((?P<macro>[^)]+)\):\s*(?P<text>.*)", re.I)
SYMBOL_RE = re.compile(r"SYMBOLGEN:\s+Macro variable\s+(?P<var>[A-Z0-9_]+)\s+resolves to\s+(?P<val>.*)", re.I)
WARNING_RE = re.compile(r"^(WARNING|ERROR):\s+(?P<msg>.*)", re.I)


def parse_sas_log(path: Path) -> Dict[str, Any]:
    reads: List[Dict[str, Any]] = []
    writes: List[Dict[str, Any]] = []
    macro_lines: List[Dict[str, Any]] = []
    symbols: Dict[str, str] = {}
    warnings: List[str] = []
    if not path.exists():
        return {"reads": reads, "writes": writes, "macro_lines": macro_lines, "symbols": symbols, "warnings": warnings}
    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
        for lineno, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            m = READ_RE.search(s)
            if m:
                reads.append({"dataset": m.group('ds').upper(), "rows": int(m.group('rows')), "lineno": lineno, "text": s})
                continue
            m = WRITE_RE.search(s)
            if m:
                writes.append({"dataset": m.group('ds').upper(), "rows": int(m.group('rows')), "cols": int(m.group('cols')), "lineno": lineno, "text": s})
                continue
            m = MPRINT_RE.search(s)
            if m:
                macro_lines.append({"macro": m.group('macro'), "text": m.group('text'), "lineno": lineno})
                continue
            m = SYMBOL_RE.search(s)
            if m:
                symbols[m.group('var').upper()] = m.group('val').strip()
                continue
            m = WARNING_RE.search(s)
            if m:
                warnings.append(s)
    return {"reads": reads, "writes": writes, "macro_lines": macro_lines, "symbols": symbols, "warnings": warnings}

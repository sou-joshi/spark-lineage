#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from app.spark_eventlog_parser import parse_eventlog_jsonl

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("eventlog", type=str)
    ap.add_argument("--out", type=str, default="graph.json")
    args = ap.parse_args()

    graph = parse_eventlog_jsonl(Path(args.eventlog))
    Path(args.out).write_text(json.dumps(graph, indent=2), encoding="utf-8")
    print(f"Wrote {args.out} (nodes={len(graph.get('nodes',[]))}, edges={len(graph.get('edges',[]))})")

if __name__ == "__main__":
    main()

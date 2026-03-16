
#!/usr/bin/env python3
import argparse, json
from pathlib import Path
from app.sas_lineage_builder import build_sas_graph

ap = argparse.ArgumentParser()
ap.add_argument('code')
ap.add_argument('--log', default='')
ap.add_argument('--out', default='sas_graph.json')
args = ap.parse_args()

g = build_sas_graph(Path(args.code), Path(args.log) if args.log else None)
Path(args.out).write_text(json.dumps(g, indent=2), encoding='utf-8')
print(f"Wrote {args.out} with {len(g['nodes'])} nodes and {len(g['edges'])} edges")

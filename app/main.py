
from fastapi import FastAPI, Request, Query, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path

from app.spark_eventlog_parser import parse_eventlog_jsonl

app = FastAPI(title="Spark Lineage Enterprise Demo (Offline)")
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")

BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLES_DIR = BASE_DIR / "samples"
UPLOADS_DIR = BASE_DIR / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)

GRAPH_CACHE = {}  # graph_key -> {"graph": {...}, "default_dataset": str, "eventlog": str}

def _load_sample_latitude():
    key = "latitude"
    if key in GRAPH_CACHE:
        return
    eventlog = SAMPLES_DIR / "latitude" / "logs" / "sample_eventlog_latitude.jsonl"
    graph = parse_eventlog_jsonl(eventlog)
    default_ds = "ORACLE.MART_LATITUDE_DAILY"
    GRAPH_CACHE[key] = {"graph": graph, "default_dataset": default_ds, "eventlog": str(eventlog)}

_load_sample_latitude()

def _filter_subgraph(graph: dict, focus_id: str, depth: int, mode: str):
    nodes = {focus_id}
    edges = graph.get("edges", [])
    adj_out = {}
    adj_in = {}
    for e in edges:
        adj_out.setdefault(e["u"], []).append(e)
        adj_in.setdefault(e["v"], []).append(e)

    def bfs(start, get_edges, direction):
        frontier = {start}
        seen = {start}
        out_edges = []
        for _ in range(depth):
            nxt = set()
            for n in frontier:
                for e in get_edges.get(n, []):
                    out_edges.append(e)
                    nxt.add(e[direction])
            nxt -= seen
            seen |= nxt
            frontier = nxt
            if not frontier:
                break
        return seen, out_edges

    up_nodes, up_edges = bfs(focus_id, adj_in, "u")
    down_nodes, down_edges = bfs(focus_id, adj_out, "v")

    if mode == "up":
        keep_nodes = up_nodes | {focus_id}
        keep_edges = up_edges
    elif mode == "down":
        keep_nodes = down_nodes | {focus_id}
        keep_edges = down_edges
    else:
        keep_nodes = (up_nodes | down_nodes | {focus_id})
        keep_edges = up_edges + down_edges

    node_map = {n["id"]: n for n in graph.get("nodes", [])}
    return {
        "nodes": [node_map[nid] for nid in keep_nodes if nid in node_map],
        "edges": [e for e in keep_edges if e["u"] in keep_nodes and e["v"] in keep_nodes],
    }

def _get_graph(graph_key: str):
    if graph_key not in GRAPH_CACHE:
        raise KeyError(graph_key)
    return GRAPH_CACHE[graph_key]["graph"]

@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    _load_sample_latitude()
    return templates.TemplateResponse(
        "home_enterprise.html",
        {
            "request": request,
            "default_ds": GRAPH_CACHE["latitude"]["default_dataset"],
            "graph_key": "latitude",
            "sample_eventlog": GRAPH_CACHE["latitude"]["eventlog"],
        },
    )

@app.post("/upload", response_class=RedirectResponse)
async def upload_eventlog(file: UploadFile = File(...), graph_key: str = Form("uploaded")):
    suffix = Path(file.filename).suffix.lower()
    if suffix not in [".jsonl", ".log", ".txt"]:
        suffix = ".jsonl"
    dest = UPLOADS_DIR / f"eventlog_{file.filename.replace('/','_')}{suffix}"
    dest.write_bytes(await file.read())

    graph = parse_eventlog_jsonl(dest)
    default_ds = "ORACLE.MART_LATITUDE_DAILY"
    ds_nodes = [n for n in graph.get("nodes", []) if n.get("type") == "dataset"]
    if ds_nodes:
        default_ds = ds_nodes[-1].get("name") or default_ds

    GRAPH_CACHE[graph_key] = {"graph": graph, "default_dataset": default_ds, "eventlog": str(dest)}
    return RedirectResponse(url=f"/lineage/table/{default_ds}?g={graph_key}", status_code=303)

@app.get("/lineage/table/{dataset:path}", response_class=HTMLResponse)
def lineage_table(request: Request, dataset: str, g: str = Query("latitude")):
    return templates.TemplateResponse("lineage_table_graph_enterprise.html", {"request": request, "dataset": dataset, "graph_key": g})

@app.get("/lineage/column", response_class=HTMLResponse)
def lineage_column(request: Request, column: str = Query(...), g: str = Query("latitude")):
    return templates.TemplateResponse("lineage_column_graph_enterprise.html", {"request": request, "column": column, "graph_key": g})

@app.get("/api/dataset/{dataset:path}", response_class=JSONResponse)
def api_dataset(dataset: str, depth: int = 2, mode: str = "all", g: str = "latitude"):
    graph = _get_graph(g)
    focus = f"ds:{dataset}"
    sub = _filter_subgraph(graph, focus, depth, mode)
    return {"graph": sub, "dataset": dataset, "graph_key": g, "eventlog": GRAPH_CACHE[g]["eventlog"]}

@app.get("/api/column", response_class=JSONResponse)
def api_column(column: str, depth: int = 2, mode: str = "all", g: str = "latitude"):
    graph = _get_graph(g)
    focus = f"col:{column}"
    sub = _filter_subgraph(graph, focus, depth, mode)
    return {"graph": sub, "column": column, "graph_key": g, "eventlog": GRAPH_CACHE[g]["eventlog"]}

@app.get("/api/meta", response_class=JSONResponse)
def api_meta(g: str = "latitude"):
    graph = _get_graph(g)
    datasets = sorted({n["name"] for n in graph.get("nodes", []) if n.get("type") == "dataset"})
    return {"graph_key": g, "datasets": datasets, "eventlog": GRAPH_CACHE[g]["eventlog"]}


AI_DEMO_QUESTIONS = [
  "Show full upstream lineage of ORACLE.MART_LATITUDE_DAILY (depth=3).",
  "Which S3 raw file paths contributed to ORACLE.MART_LATITUDE_DAILY?",
  "List transformations between ORACLE.ODS_LATITUDE and ORACLE.MART_LATITUDE_DAILY.",
  "How is ORACLE.MART_LATITUDE_DAILY.LAT_BAND derived?",
  "How is ORACLE.MART_LATITUDE_DAILY.PING_COUNT calculated?",
  "Show downstream impact if ORACLE.ODS_LATITUDE schema changes (which tables/columns depend on it).",
  "Which columns are derived using UDFs?",
  "Which columns are aggregated in the MART layer?",
  "What is the transformation used to create ORACLE.STG_LATITUDE.MSISDN_HASH?",
  "Show lineage chain from S3 RAW to ORACLE MART for LAT_NUMBER.",
  "Explain the full data journey for ORACLE.MART_LATITUDE_DAILY in simple terms.",
  "Which datasets sit in each layer (RAW, CONFORMED, ODS, STG, MART)?",
  "Which joins are present in the STAGING build?",
  "What are the top 5 upstream datasets for ORACLE.MART_LATITUDE_DAILY?",
  "Show the immediate upstream columns for ORACLE.MART_LATITUDE_DAILY.AVG_LAT.",
  "Does any transformation normalize/round latitude values? Where?",
  "If the raw column name changes from lat_number to latitude, what breaks?",
  "Which columns are pass-through vs derived in ORACLE.MART_LATITUDE_DAILY?",
  "Show evidence for how LAT_BAND was computed.",
  "Summarize all derived columns in ORACLE.MART_LATITUDE_DAILY with expressions."
]

def _id_for_dataset(name: str) -> str:
    return f"ds:{name}"

def _id_for_column(full: str) -> str:
    return f"col:{full}"

def _graph_for_key(graph_key: str):
    _load_sample_latitude()
    return GRAPH_CACHE.get(graph_key) or GRAPH_CACHE.get("latitude")

def _edges_into(graph: dict, node_id: str):
    return [e for e in (graph.get("edges") or []) if e.get("v")==node_id and e.get("edge_type")=="column"]

def _edges_out(graph: dict, node_id: str):
    return [e for e in (graph.get("edges") or []) if e.get("u")==node_id and e.get("edge_type")=="column"]

def _datasets_by_kind(graph: dict):
    ds = [n for n in (graph.get("nodes") or []) if n.get("type")=="dataset"]
    return ds

def _layer(name: str) -> str:
    u = (name or "").upper()
    if u.startswith("S3://") and "/RAW/" in u:
        return "S3 RAW"
    if u.startswith("S3://") and ("CONFORM" in u or "CONFORMED" in u):
        return "S3 CONFORMED"
    if "ODS" in u:
        return "ORACLE ODS"
    if "STG" in u or "STAGING" in u:
        return "ORACLE STAGING"
    if "MART" in u:
        return "ORACLE MART"
    if u.startswith("S3://"):
        return "S3"
    if "ORACLE" in u:
        return "ORACLE"
    return "OTHER"

def _ai_answer(question: str, graph_key: str):
    # AI demo response grounded in lineage + transformations based on the parsed lineage graph.
    q = (question or "").strip()
    gctx = _graph_for_key(graph_key)
    graph = gctx["graph"]
    # Helpers
    def find_dataset_name():
        m = re.search(r"(ORACLE\.[A-Z0-9_\.]+|S3://[^\s)]+)", q, re.I)
        return m.group(1) if m else gctx.get("default_dataset")

    def find_column_full():
        m = re.search(r"(ORACLE\.[A-Z0-9_]+\.[A-Z0-9_]+|S3://[^\s]+\.[A-Z0-9_]+)", q, re.I)
        return m.group(1) if m else None

    ds_name = find_dataset_name()
    col_full = find_column_full()

    # Derived column questions
    if ("how is" in q.lower() or "derived" in q.lower() or "computed" in q.lower()) and col_full:
        cid = _id_for_column(col_full)
        incoming = _edges_into(graph, cid)
        if not incoming:
            return {
              "answer": f"I couldn't find a derived lineage edge into {col_full} in this demo graph. It may be pass-through or not captured.",
              "references": []
            }
        # Prefer edges with transformation metadata
        incoming_sorted = sorted(incoming, key=lambda e: float(e.get("confidence") or 0), reverse=True)
        lines = []
        refs = []
        for e in incoming_sorted[:6]:
            t = e.get("transformation") or {}
            kind = t.get("kind") or "expr"
            expr = t.get("expr") or e.get("evidence") or "—"
            udf = t.get("udf")
            from_col = (e.get("u") or "").replace("col:","")
            lines.append(f"- From **{from_col}** → via **{kind}**" + (f" (**{udf}**)" if udf else "") + f"  \n  ` {expr} `")
            refs.append({"edge": {"u": e.get("u"), "v": e.get("v"), "evidence": e.get("evidence"), "confidence": e.get("confidence")}})
        return {"answer": f"**{col_full}** is derived as follows:\n" + "\n".join(lines), "references": refs}

    # Transformations between layers
    if "transformations" in q.lower() and ds_name:
        # list column edges that have transformation metadata and are part of path to ds_name
        tgt = _id_for_dataset(ds_name)
        # capture dataset chain edges (dataset edge_type)
        ds_edges = [e for e in (graph.get("edges") or []) if e.get("edge_type")=="dataset"]
        # simple: list transformations where target_col begins with ds_name
        trans_edges = [e for e in (graph.get("edges") or []) if e.get("edge_type")=="column" and (e.get("transformation") or {}).get("target_col","").upper().startswith(ds_name.upper())]
        if not trans_edges:
            return {"answer": f"No explicit transformation metadata found for {ds_name} in this demo.", "references":[]}
        out=[]
        for e in sorted(trans_edges, key=lambda x: float(x.get("confidence") or 0), reverse=True)[:12]:
            t = e.get("transformation") or {}
            out.append(f"- **{t.get('target_col')}** ← `{t.get('expr')}`" + (f" (UDF: {t.get('udf')})" if t.get("udf") else ""))
        return {"answer": "Here are the captured transformations:\n" + "\n".join(out), "references":[]}

    # Upstream lineage summary
    if "upstream" in q.lower() and ds_name:
        did = _id_for_dataset(ds_name)
        sub = _filter_subgraph(graph, did, depth=3, mode="up")
        ds_nodes = [n for n in (sub["nodes"] or []) if n.get("type")=="dataset"]
        ds_nodes_sorted = sorted(ds_nodes, key=lambda n: _layer(n.get("name","")) )
        chain = []
        for n in ds_nodes_sorted:
            chain.append(f"- {_layer(n['name'])}: **{n['name']}**")
        return {"answer": f"Upstream datasets for **{ds_name}** (depth=3):\n" + "\n".join(chain), "references":[]}

    # S3 raw paths
    if ("s3" in q.lower() and ("path" in q.lower() or "file" in q.lower())) and ds_name:
        did=_id_for_dataset(ds_name)
        sub=_filter_subgraph(graph, did, depth=5, mode="up")
        s3=[n["name"] for n in (sub["nodes"] or []) if n.get("type")=="dataset" and str(n.get("name","")).lower().startswith("s3://")]
        s3=sorted(set(s3))
        if not s3:
            return {"answer":"No S3 datasets found upstream in this demo graph.", "references":[]}
        return {"answer":"Upstream S3 datasets:\n" + "\n".join([f"- `{x}`" for x in s3[:25]]), "references":[]}

    # Summarize journey
    if "journey" in q.lower() or "simple terms" in q.lower():
        return {"answer": "Data journey (demo):\n- S3 RAW CSV → validated and enriched\n- Written as S3 CONFORMED Parquet\n- Loaded to ORACLE ODS (operational store)\n- Cleaned/enriched to ORACLE STAGING (hashing + normalization)\n- Aggregated into ORACLE MART for analytics", "references":[]}

    # Default
    return {"answer": "Try clicking one of the 20 demo questions or ask about a specific dataset/column (e.g., ORACLE.MART_LATITUDE_DAILY.LAT_BAND).", "references":[]}

@app.get("/ai", response_class=HTMLResponse)
def ai_demo(request: Request, g: str = Query("latitude")):
    _load_sample_latitude()
    ctx = _graph_for_key(g)
    return templates.TemplateResponse("ai_demo.html", {
        "request": request,
        "graph_key": g,
        "questions": AI_DEMO_QUESTIONS,
        "default_ds": ctx.get("default_dataset"),
    })

@app.post("/api/ai/ask")
async def ai_ask(request: Request):
    body = await request.json()
    q = body.get("question") or ""
    g = body.get("graph_key") or "latitude"
    return JSONResponse(_ai_answer(q, g))

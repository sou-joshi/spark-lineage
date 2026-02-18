
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

import os
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.store import Store
from app.spark_ingest import ingest_spark_eventlog
from app.spark_driver_log import ingest_driver_log
from app.qa import answer_question

APP_TITLE = "Spark Lineage MVP (Offline AI)"
DATA_DIR = os.environ.get("DATA_DIR", "data")
DB_PATH = os.environ.get("DB_PATH", os.path.join(DATA_DIR, "lineage.sqlite"))
MODEL_PATH = os.environ.get("MODEL_PATH", os.path.join(DATA_DIR, "models", "model.gguf"))

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "logs"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "models"), exist_ok=True)

store = Store(DB_PATH)

app = FastAPI(title=APP_TITLE)
templates = Jinja2Templates(directory="app/templates")
app.mount("/static", StaticFiles(directory="app/static"), name="static")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    stats = store.get_stats()
    return templates.TemplateResponse("index.html", {"request": request, "stats": stats})


@app.post("/upload", response_class=HTMLResponse)
async def upload_log(request: Request, file: UploadFile = File(...)):
    out_path = os.path.join(DATA_DIR, "logs", file.filename)
    with open(out_path, "wb") as f:
        f.write(await file.read())
    report = ingest_spark_eventlog(out_path, store)
    return templates.TemplateResponse("upload_result.html", {"request": request, "path": out_path, "report": report})


@app.post("/upload_driver_log", response_class=HTMLResponse)
async def upload_driver_log(request: Request, file: UploadFile = File(...)):
    out_path = os.path.join(DATA_DIR, "logs", file.filename)
    with open(out_path, "wb") as f:
        f.write(await file.read())
    report = ingest_driver_log(out_path, store)
    return templates.TemplateResponse("upload_result.html", {"request": request, "path": out_path, "report": report})


@app.get("/search", response_class=HTMLResponse)
def search(request: Request, q: str = ""):
    results = store.search(q) if q else {"tables": [], "columns": []}
    return templates.TemplateResponse("search.html", {"request": request, "q": q, "results": results})


@app.get("/lineage/table/{dataset_name}", response_class=HTMLResponse)
def table_lineage(request: Request, dataset_name: str, depth: int = 2):
    g = store.build_graph()
    view = store.get_dataset_lineage_view(g, dataset_name, depth=depth)
    if view.get("not_found"):
        return templates.TemplateResponse("not_found.html", {"request": request, "name": dataset_name, "kind": "dataset"})
    return templates.TemplateResponse("lineage_table.html", {"request": request, "view": view})


@app.get("/lineage/column", response_class=HTMLResponse)
def column_lineage(request: Request, column: str, depth: int = 2):
    g = store.build_graph()
    view = store.get_column_lineage_view(g, column, depth=depth)
    if view.get("not_found"):
        return templates.TemplateResponse("not_found.html", {"request": request, "name": column, "kind": "column"})
    return templates.TemplateResponse("lineage_column.html", {"request": request, "view": view})


@app.get("/api/dataset/{dataset_name}")
def api_dataset(dataset_name: str, depth: int = 2):
    g = store.build_graph()
    return JSONResponse(store.get_dataset_lineage_view(g, dataset_name, depth=depth))


@app.get("/api/column")
def api_column(column: str, depth: int = 2):
    g = store.build_graph()
    return JSONResponse(store.get_column_lineage_view(g, column, depth=depth))


@app.get("/chat", response_class=HTMLResponse)
def chat_page(request: Request):
    return templates.TemplateResponse("chat.html", {"request": request, "question": "", "answer": None})


@app.post("/chat", response_class=HTMLResponse)
async def chat(request: Request, question: str = Form(...)):
    g = store.build_graph()
    ans = answer_question(question=question, graph=g, store=store, model_path=MODEL_PATH)
    return templates.TemplateResponse("chat.html", {"request": request, "question": question, "answer": ans})


@app.post("/reset")
def reset():
    store.reset()
    return RedirectResponse("/", status_code=303)

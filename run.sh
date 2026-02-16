#!/usr/bin/env bash
set -euo pipefail

export DATA_DIR="${DATA_DIR:-data}"
export DB_PATH="${DB_PATH:-$DATA_DIR/lineage.sqlite}"
export MODEL_PATH="${MODEL_PATH:-$DATA_DIR/models/model.gguf}"

uvicorn app.main:app --host 0.0.0.0 --port 8000

#!/bin/bash
echo "Starting FastAPI server..."
uvicorn app:app --reload &
sleep 2
echo "Running pytest..."
pytest -q
echo "Running validation script..."
python scripts/validate_mapping.py
echo "Demo complete. Open http://127.0.0.1:8000/docs for manual exploration."

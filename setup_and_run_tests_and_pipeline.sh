#!/bin/bash

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
pytest && python src/transactions_pipeline.py
deactivate

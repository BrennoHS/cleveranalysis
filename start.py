#!/usr/bin/env python
"""
Start the AdOps Discrepancy Tool server.
Run this from the project root: python start.py
"""

import os
import sys
import uvicorn

# Change to backend directory
os.chdir(os.path.join(os.path.dirname(__file__), 'backend'))
sys.path.insert(0, os.getcwd())

if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000)

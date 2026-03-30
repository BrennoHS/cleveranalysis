#!/usr/bin/env python
"""
Start the AdOps Discrepancy Tool server.
Run this from the project root: python start.py
"""

import os
import sys
import uvicorn


def ensure_supported_python() -> None:
    """Fail fast on unsupported Python versions with a clear instruction."""
    # google-generativeai/protobuf currently fails on Python 3.14 in this project.
    if sys.version_info >= (3, 14):
        print("This project is not compatible with Python 3.14 yet.")
        print("Use Python 3.12 to run it:")
        print("  py -3.12 -m pip install -r backend/requirements.txt")
        print("  py -3.12 start.py")
        raise SystemExit(1)

# Change to backend directory
os.chdir(os.path.join(os.path.dirname(__file__), 'backend'))
sys.path.insert(0, os.getcwd())

if __name__ == "__main__":
    ensure_supported_python()
    uvicorn.run("main:app", host="127.0.0.1", port=8000)

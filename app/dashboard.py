"""Streamlit script entrypoint; run from a checkout or an installed package."""

import sys
from pathlib import Path

# Streamlit executes this file as a script, so expose its containing package once.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.views import main

main()

# Offline dashboard

```bash
pip install -r app/requirements.txt
streamlit run app/dashboard.py
```

Run from the repository root. `dashboard.py` renders three decision tabs; `dashboard_data.py` reads and calculates metrics from the verified local Gold bundle. Neither requires AWS at runtime.

For Streamlit Community Cloud, select `app/dashboard.py` and Python 3.11. The adjacent `requirements.txt` installs the dashboard stack. On first startup, the app downloads a checksum-pinned Gold archive from the public GitHub data release if the default local bundle is missing. The installed bundle is reused for interactions and shared across sessions. A host rebuild or disk reset may download it again; S3 is never used by the dashboard. `GOLD_BUNDLE_DIR` explicitly selects a supplied local bundle instead.

[Setup, data packaging and metric rules](../docs/runbooks/DASHBOARD.md)

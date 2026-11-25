# Offline dashboard

```bash
pip install -r app/requirements.txt
streamlit run app/dashboard.py
```

Run from the repository root. `dashboard.py` renders three decision tabs; `dashboard_data.py` reads and calculates metrics from the verified local Gold bundle. Neither requires AWS at runtime.

For Streamlit Community Cloud, select `app/dashboard.py` and Python 3.11. The adjacent `requirements.txt` installs the dashboard stack. The real Gold bundle is gitignored and must be supplied separately for hosting.

[Setup, data packaging and metric rules](../docs/runbooks/DASHBOARD.md)

import json
from pathlib import Path
import pandas as pd
import streamlit as st

st.title("GPU fleet activity")
files = sorted(Path("output/gold").glob("*/*/summary.json"))
upload = st.file_uploader("Open a Gold summary from S3", type="json")
if upload:
    report = json.load(upload)
elif files:
    report = json.loads(st.selectbox("Local trace day", files).read_text())
else:
    st.info("Upload a summary.json or run the pipeline locally to see results.")
    st.stop()
scope = "all trace days" if report["day"] is None else f"trace day {report['day']}"
st.subheader(f"{report['dataset'].title()} activity · {scope}")
frame = pd.DataFrame(report["rows"])
if frame.empty:
    st.info("No observations for this selection.")
elif report["dataset"] == "network":
    st.line_chart(frame.set_index("hour")[["mean_rx_gibps", "mean_tx_gibps"]])
    st.caption("Mean receive and transmit traffic per observed server, in GiB/s.")
elif report["dataset"] == "server":
    st.bar_chart(frame.set_index("gpu_spec_public")["observed_gpu_hours"])
    st.caption("Observed GPU-hours across hourly server inventory records; not workload utilization.")
elif report["dataset"] == "execution":
    st.bar_chart(frame.set_index("job_type_public")[["mean_duration_hours", "median_duration_hours", "p95_duration_hours"]])
    st.caption("Execution durations in hours across the complete execution summary.")
else:
    st.bar_chart(frame.set_index("job_type_public")["mean_sm_util"])
    st.caption("Mean reported SM utilization across measured pod-hours with a positive GPU request.")
st.dataframe(frame)

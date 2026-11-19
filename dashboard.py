import json
from pathlib import Path
import pandas as pd
import streamlit as st

st.title("GPU fleet activity")
files = sorted(Path("output/gold").glob("*/day=*/summary.json"))
upload = st.file_uploader("Open a Gold summary from S3", type="json")
if upload:
    report = json.load(upload)
elif files:
    report = json.loads(st.selectbox("Local trace day", files).read_text())
else:
    st.info("Upload a summary.json or run the pipeline locally to see results.")
    st.stop()
st.subheader(f"{report['dataset'].title()} activity · trace day {report['day']}")
frame = pd.DataFrame(report["rows"])
if frame.empty:
    st.info("No observations for this selection.")
elif report["dataset"] == "network":
    st.line_chart(frame.set_index("hour")[["mean_rx_gibps", "mean_tx_gibps"]])
    st.caption("Mean receive and transmit traffic per observed server, in GiB/s.")
else:
    st.bar_chart(frame.set_index("job_type_public")["mean_sm_util"])
    st.caption("Mean reported SM utilization across measured pod-hours with a positive GPU request.")
st.dataframe(frame)

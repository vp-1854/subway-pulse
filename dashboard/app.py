"""Subway Pulse dashboard: service status page.

Checks that the serving layer and both databases are reachable. The live
congestion map, station table, history and forecast views build on this page.
"""
import os

import requests
import streamlit as st

API_URL = os.environ.get("SUBWAY_PULSE_API_URL", "http://localhost:8000")

st.set_page_config(page_title="Subway Pulse", layout="wide")
st.title("Subway Pulse")
st.caption("Real-time MTA subway congestion")


def fetch_health() -> dict:
    try:
        resp = requests.get(f"{API_URL}/health", timeout=5)
        return resp.json()
    except Exception as exc:
        return {"status": "unreachable", "error": str(exc)}


health = fetch_health()
labels = {"mongo": "MongoDB", "cassandra": "Cassandra", "status": "API"}
for col, (key, label) in zip(st.columns(len(labels)), labels.items()):
    col.metric(label, "OK" if health.get(key) == "ok" else "Down")

if health.get("status") != "ok":
    st.warning("Some services are not reachable yet. Start the stack and the API, then reload.")

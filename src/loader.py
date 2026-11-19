import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import pandas as pd
import pm4py

CORE_COLUMNS = ["case:concept:name", "concept:name", "time:timestamp"]

CSV_COLUMN_MAP = {
    "Order_ID": "case:concept:name",
    "case": "case:concept:name",
    "case concept:name": "case:concept:name",
    "Case ID": "case:concept:name",
    "Action_Step": "concept:name",
    "event": "concept:name",
    "event concept:name": "concept:name",
    "Activity": "concept:name",
    "Timestamp": "time:timestamp",
    "startTime": "time:timestamp",
    "event time:timestamp": "time:timestamp",
    "Complete Timestamp": "time:timestamp",
}


def configure_graphviz(windows_bin: str = r"C:\Program Files\Graphviz\bin") -> None:
    """Append default Windows Graphviz path if present and not already on PATH."""
    if os.path.isdir(windows_bin) and windows_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] += os.pathsep + windows_bin


def normalize_erp_csv(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize heterogeneous ERP CSV columns into a sorted PM4Py event DataFrame."""
    renamed = df.rename(columns={k: v for k, v in CSV_COLUMN_MAP.items() if k in df.columns}).copy()
    missing = [c for c in CORE_COLUMNS if c not in renamed.columns]
    if missing:
        raise ValueError(f"Missing required PM4Py columns after mapping: {missing}. Found: {list(df.columns)}")

    renamed["time:timestamp"] = pd.to_datetime(renamed["time:timestamp"], format="mixed", utc=True)
    renamed = renamed.sort_values(by=["case:concept:name", "time:timestamp"]).reset_index(drop=True)
    return pm4py.format_dataframe(
        renamed,
        case_id="case:concept:name",
        activity_key="concept:name",
        timestamp_key="time:timestamp",
    )


def stream_xes_core(input_path: Path) -> pd.DataFrame:
    """
    Low-memory streaming XES parser using C-accelerated ElementTree.iterparse.
    Projects only the 3 core process-mining columns and clears XML nodes per trace
    to prevent full-DOM object-matrix OOM on 700+ MB enterprise ERP logs.
    """
    case_ids: list[str] = []
    activities: list[str] = []
    timestamps: list[str] = []

    root = None
    in_trace = False
    in_event = False
    case_id: str | None = None
    ev_act: str | None = None
    ev_ts: str | None = None
    trace_events: list[tuple[str, str]] = []

    for event, elem in ET.iterparse(str(input_path), events=("start", "end")):
        if root is None and event == "start":
            root = elem

        tag = elem.tag.rsplit("}", 1)[-1]

        if event == "start":
            if tag == "trace":
                in_trace = True
                case_id = None
                trace_events = []
            elif tag == "event" and in_trace:
                in_event = True
                ev_act = None
                ev_ts = None
        else:
            if tag == "string":
                if elem.attrib.get("key") == "concept:name":
                    val = elem.attrib.get("value")
                    if in_event:
                        ev_act = val
                    elif in_trace:
                        case_id = val
                elem.clear()
            elif tag == "date":
                if in_event and elem.attrib.get("key") == "time:timestamp":
                    ev_ts = elem.attrib.get("value")
                elem.clear()
            elif tag == "event" and in_trace:
                if ev_act is not None and ev_ts is not None:
                    trace_events.append((ev_act, ev_ts))
                in_event = False
                elem.clear()
            elif tag == "trace":
                if case_id is not None and trace_events:
                    for act, ts in trace_events:
                        case_ids.append(case_id)
                        activities.append(act)
                        timestamps.append(ts)
                in_trace = False
                elem.clear()
                if root is not None:
                    root.clear()
            elif tag not in ("log",):
                elem.clear()

    try:
        parsed_ts = pd.to_datetime(timestamps, format="ISO8601", utc=True)
    except ValueError:
        parsed_ts = pd.to_datetime(timestamps, format="mixed", utc=True)

    df = pd.DataFrame(
        {
            "case:concept:name": case_ids,
            "concept:name": activities,
            "time:timestamp": parsed_ts,
        }
    )
    return df.sort_values(by=["case:concept:name", "time:timestamp"]).reset_index(drop=True)


def load_event_log(input_path: Path, use_cache: bool = True) -> tuple[pd.DataFrame, float, str]:
    """
    Single-pass loader for .xes or .csv ERP logs.
    Caches the 3 core PM4Py columns as a local pickle for fast iterative benchmarking.
    """
    t0 = time.perf_counter()
    cache_path = input_path.with_suffix(".core.pkl")

    if use_cache and input_path.suffix.lower() == ".xes" and cache_path.exists():
        df = pd.read_pickle(cache_path)
        return df, (time.perf_counter() - t0) * 1000.0, f"cache ({cache_path.name})"

    if input_path.suffix.lower() == ".xes":
        df = stream_xes_core(input_path)
        if use_cache:
            df.to_pickle(cache_path)
        source_label = f"xes-stream ({input_path.name})"
    elif input_path.suffix.lower() == ".csv":
        df = normalize_erp_csv(pd.read_csv(input_path))
        source_label = f"csv ({input_path.name})"
    else:
        raise ValueError(f"Unsupported event log format: {input_path.suffix}")

    return df, (time.perf_counter() - t0) * 1000.0, source_label
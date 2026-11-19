from pathlib import Path
import pandas as pd
import pytest

from src.analytics import benchmark_discovery_miners, characterize_log, profile_bottlenecks
from src.loader import load_event_log, normalize_erp_csv
from src.visualize import export_bottleneck_dashboard


def _build_synthetic_p2p_df() -> pd.DataFrame:
    """Minimal synthetic Procure-to-Pay event log (5 cases, 2 trace variants)."""
    rows = []
    # Variant 1 (4 cases): Create PO -> Goods Receipt (2d) -> Invoice Receipt (5d) -> Clear Invoice (1d)
    for idx in range(1, 5):
        cid = f"PO-{idx:03d}"
        base = pd.Timestamp("2026-01-01T08:00:00Z") + pd.Timedelta(days=idx)
        rows.extend(
            [
                {"Order_ID": cid, "Activity": "Create PO", "Timestamp": base},
                {"Order_ID": cid, "Activity": "Goods Receipt", "Timestamp": base + pd.Timedelta(days=2)},
                {"Order_ID": cid, "Activity": "Invoice Receipt", "Timestamp": base + pd.Timedelta(days=7)},
                {"Order_ID": cid, "Activity": "Clear Invoice", "Timestamp": base + pd.Timedelta(days=8)},
            ]
        )

    # Variant 2 (1 rework case): Create PO -> Change Price -> Goods Receipt -> Invoice Receipt -> Clear Invoice
    base = pd.Timestamp("2026-01-10T08:00:00Z")
    rows.extend(
        [
            {"Order_ID": "PO-999", "Activity": "Create PO", "Timestamp": base},
            {"Order_ID": "PO-999", "Activity": "Change Price", "Timestamp": base + pd.Timedelta(days=1)},
            {"Order_ID": "PO-999", "Activity": "Goods Receipt", "Timestamp": base + pd.Timedelta(days=3)},
            {"Order_ID": "PO-999", "Activity": "Invoice Receipt", "Timestamp": base + pd.Timedelta(days=9)},
            {"Order_ID": "PO-999", "Activity": "Clear Invoice", "Timestamp": base + pd.Timedelta(days=10)},
        ]
    )
    return normalize_erp_csv(pd.DataFrame(rows))


def test_normalize_erp_csv_maps_heterogeneous_columns():
    raw = pd.DataFrame(
        {
            "Order_ID": ["PO-1", "PO-1"],
            "Action_Step": ["Create PO", "Approve PO"],
            "Complete Timestamp": ["2026-01-02 12:00:00", "2026-01-01 09:00:00"],
        }
    )
    df = normalize_erp_csv(raw)
    assert {"case:concept:name", "concept:name", "time:timestamp"}.issubset(df.columns)
    # Chronological sorting puts 2026-01-01 before 2026-01-02
    assert df.iloc[0]["concept:name"] == "Approve PO"


def test_normalize_erp_csv_raises_on_missing_core_columns():
    bad = pd.DataFrame({"unknown_id": ["1"], "step": ["A"]})
    with pytest.raises(ValueError, match="Missing required PM4Py columns"):
        normalize_erp_csv(bad)


def test_characterize_log_computes_variants_entropy_and_coverage():
    df = _build_synthetic_p2p_df()
    stats = characterize_log(df, top_k_levels=(1, 2))

    assert stats["total_cases"] == 5
    assert stats["total_events"] == 21
    assert stats["unique_activities"] == 5
    assert stats["unique_variants"] == 2
    assert stats["top_k_coverage_pct"][1] == 80.0
    assert stats["top_k_coverage_pct"][2] == 100.0
    assert 0.0 < stats["normalized_entropy"] < 1.0


def test_benchmark_discovery_miners_returns_valid_conformance_metrics():
    df = _build_synthetic_p2p_df()
    rows, models = benchmark_discovery_miners(df)

    assert len(rows) == 3
    assert len(models) == 3
    for r in rows:
        assert 0.0 <= r["fitness"] <= 1.0
        assert 0.0 <= r["precision"] <= 1.0
        assert 0.0 <= r["f1_score"] <= 1.0
        assert 0.0 <= r["simplicity"] <= 1.0
        assert r["places"] > 0 and r["transitions"] > 0


def test_profile_bottlenecks_identifies_slowest_transition():
    df = _build_synthetic_p2p_df()
    cycle_stats, top_bn, case_days = profile_bottlenecks(df, min_transition_freq=2, top_n=5)

    assert cycle_stats["cases"] == 5
    assert cycle_stats["median_days"] == 8.0
    # Goods Receipt -> Invoice Receipt takes 5–6 days (slowest edge)
    assert top_bn.iloc[0]["edge"] == "Goods Receipt -> Invoice Receipt"
    assert top_bn.iloc[0]["median_days"] == 5.0
    assert len(case_days) == 5


def test_export_bottleneck_dashboard_creates_compact_png(tmp_path: Path):
    df = _build_synthetic_p2p_df()
    cycle_stats, top_bn, case_days = profile_bottlenecks(df, min_transition_freq=1, top_n=4)

    out_png = tmp_path / "dashboard.png"
    export_bottleneck_dashboard(case_days, top_bn, cycle_stats, out_png)
    assert out_png.exists()
    assert 5_000 < out_png.stat().st_size < 500_000

def test_stream_xes_core_parses_standard_xes_and_caches(tmp_path: Path):
    xes_file = tmp_path / "mini.xes"
    xes_file.write_text(
        """<?xml version="1.0" encoding="UTF-8"?>
        <log xes.version="1.0" xmlns="http://www.xes-standard.org/">
          <trace>
            <string key="concept:name" value="PO-101"/>
            <event>
              <string key="concept:name" value="Create PO"/>
              <date key="time:timestamp" value="2026-01-01T10:00:00.000+00:00"/>
              <string key="ignored_attr" value="dropped"/>
            </event>
            <event>
              <string key="concept:name" value="Clear Invoice"/>
              <date key="time:timestamp" value="2026-01-03T10:00:00.000+00:00"/>
            </event>
          </trace>
        </log>""",
        encoding="utf-8",
    )
    df, _, source = load_event_log(xes_file, use_cache=True)
    assert "xes-stream" in source
    assert len(df) == 2
    assert list(df["concept:name"]) == ["Create PO", "Clear Invoice"]

    # Second load hits local .core.pkl cache
    df_cached, _, source_cached = load_event_log(xes_file, use_cache=True)
    assert "cache" in source_cached
    assert len(df_cached) == 2
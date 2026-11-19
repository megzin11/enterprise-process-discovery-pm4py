from collections import Counter
import math
import time
from typing import Any
import pandas as pd
import pm4py


def _extract_case_traces(df: pd.DataFrame) -> dict[str, tuple[str, ...]]:
    """Single-pass extraction of ordered activity tuples per case (PyArrow-safe)."""
    sorted_df = df.sort_values(["case:concept:name", "time:timestamp"])
    case_traces: dict[str, tuple[str, ...]] = {}
    current_case = None
    current_acts: list[str] = []

    for cid, act in zip(
        sorted_df["case:concept:name"].tolist(),
        sorted_df["concept:name"].tolist(),
    ):
        if cid != current_case:
            if current_case is not None:
                case_traces[current_case] = tuple(current_acts)
            current_case = cid
            current_acts = [act]
        else:
            current_acts.append(act)

    if current_case is not None:
        case_traces[current_case] = tuple(current_acts)

    return case_traces


def filter_top_k_variants(df: pd.DataFrame, top_k: int) -> pd.DataFrame:
    """Keep all cases belonging to the top-k most frequent trace variants."""
    case_traces = _extract_case_traces(df)
    variant_counter = Counter(case_traces.values())
    top_variants = {v for v, _ in variant_counter.most_common(top_k)}
    keep_cases = {cid for cid, trace in case_traces.items() if trace in top_variants}
    return df[df["case:concept:name"].isin(keep_cases)].copy()


def characterize_log(df: pd.DataFrame, top_k_levels: tuple[int, ...] = (5, 10, 20, 50)) -> dict[str, Any]:
    """
    Compute pre-mining log complexity metrics to quantify ERP variant noise
    and empirically justify trace filtering thresholds.
    """
    total_events = len(df)
    case_traces = _extract_case_traces(df)
    total_cases = len(case_traces)
    unique_activities = int(df["concept:name"].nunique())

    variant_counter = Counter(case_traces.values())
    unique_variants = len(variant_counter)
    counts_desc = sorted(variant_counter.values(), reverse=True)

    # Shannon entropy of trace variant distribution (normalized 0.0 -> 1.0)
    entropy = float(-sum((c / total_cases) * math.log2(c / total_cases) for c in counts_desc)) if total_cases else 0.0
    max_entropy = math.log2(unique_variants) if unique_variants > 1 else 1.0
    normalized_entropy = entropy / max_entropy if unique_variants > 1 else 0.0

    coverage = {
        k: round(float(sum(counts_desc[:k]) / total_cases) * 100.0, 2) if total_cases else 0.0
        for k in top_k_levels
    }

    return {
        "total_cases": total_cases,
        "total_events": total_events,
        "avg_events_per_case": round(total_events / total_cases, 2) if total_cases else 0.0,
        "unique_activities": unique_activities,
        "unique_variants": unique_variants,
        "variant_to_case_ratio": round(unique_variants / total_cases, 4) if total_cases else 0.0,
        "variant_entropy_bits": round(entropy, 3),
        "normalized_entropy": round(normalized_entropy, 3),
        "top_k_coverage_pct": coverage,
    }

def benchmark_discovery_miners(filtered_df: pd.DataFrame) -> tuple[list[dict[str, Any]], dict[str, tuple]]:
    """
    Benchmark Inductive Miner (noise=0.0, noise=0.2) and Heuristics Miner (dep=0.5)
    using Token-Based Replay fitness, precision, harmonic F1, and arc-degree simplicity.
    """
    configs = [
        ("Inductive Miner (noise=0.0)", lambda log: pm4py.discover_petri_net_inductive(log, noise_threshold=0.0)),
        ("Inductive Miner (noise=0.2)", lambda log: pm4py.discover_petri_net_inductive(log, noise_threshold=0.2)),
        ("Heuristics Miner (dep=0.5)", lambda log: pm4py.discover_petri_net_heuristics(log, dependency_threshold=0.5)),
    ]

    results: list[dict[str, Any]] = []
    models: dict[str, tuple] = {}

    for name, discover_fn in configs:
        t0 = time.perf_counter()
        net, im, fm = discover_fn(filtered_df)
        discover_ms = (time.perf_counter() - t0) * 1000.0

        t_eval = time.perf_counter()
        fitness_dict = pm4py.fitness_token_based_replay(filtered_df, net, im, fm)
        fitness = float(fitness_dict["log_fitness"])
        precision = float(pm4py.precision_token_based_replay(filtered_df, net, im, fm))
        simplicity = float(pm4py.simplicity_petri_net(net, im, fm))
        f1 = (2 * fitness * precision / (fitness + precision)) if (fitness + precision) > 0 else 0.0
        eval_ms = (time.perf_counter() - t_eval) * 1000.0

        results.append(
            {
                "algorithm": name,
                "fitness": round(fitness, 4),
                "precision": round(precision, 4),
                "f1_score": round(f1, 4),
                "simplicity": round(simplicity, 4),
                "places": len(net.places),
                "transitions": len(net.transitions),
                "arcs": len(net.arcs),
                "discover_ms": round(discover_ms, 1),
                "eval_ms": round(eval_ms, 1),
            }
        )
        models[name] = (net, im, fm)

    return results, models


def profile_bottlenecks(
    df: pd.DataFrame,
    min_transition_freq: int = 500,
    top_n: int = 8,
) -> tuple[dict[str, float], pd.DataFrame, pd.Series]:
    """
    Compute case cycle-time percentiles (in days) and top high-volume activity transitions
    ranked by median wait time using vectorized Pandas operations.
    """
    ordered = df.sort_values(["case:concept:name", "time:timestamp"])

    # Case-level end-to-end cycle time (days)
    case_bounds = ordered.groupby("case:concept:name", sort=False)["time:timestamp"].agg(["min", "max"])
    case_days = (case_bounds["max"] - case_bounds["min"]).dt.total_seconds() / 86400.0

    cycle_stats = {
        "cases": int(len(case_days)),
        "mean_days": round(float(case_days.mean()), 2) if len(case_days) else 0.0,
        "median_days": round(float(case_days.median()), 2) if len(case_days) else 0.0,
        "p90_days": round(float(case_days.quantile(0.90)), 2) if len(case_days) else 0.0,
        "p95_days": round(float(case_days.quantile(0.95)), 2) if len(case_days) else 0.0,
    }

    # Transition-level waiting time between consecutive events within the same case
    next_case = ordered["case:concept:name"].shift(-1)
    next_act = ordered["concept:name"].shift(-1)
    next_ts = ordered["time:timestamp"].shift(-1)

    same_case = ordered["case:concept:name"].eq(next_case)
    transitions = pd.DataFrame(
        {
            "source": ordered.loc[same_case, "concept:name"].values,
            "target": next_act.loc[same_case].values,
            "wait_days": (next_ts.loc[same_case] - ordered.loc[same_case, "time:timestamp"]).dt.total_seconds().values / 86400.0,
        }
    )
    transitions = transitions[transitions["wait_days"] >= 0]
    transitions["edge"] = transitions["source"] + " -> " + transitions["target"]

    agg = (
        transitions.groupby("edge")["wait_days"]
        .agg(count="count", median_days="median", mean_days="mean")
        .reset_index()
    )
    # Apply frequency floor so 1-off anomalies don't hijack the bottleneck ranking
    frequent = agg[agg["count"] >= min_transition_freq]
    if frequent.empty:
        frequent = agg
    top_bottlenecks = frequent.sort_values("median_days", ascending=False).head(top_n).reset_index(drop=True)

    return cycle_stats, top_bottlenecks, case_days
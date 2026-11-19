import argparse
import time
from pathlib import Path
import pm4py
from src.analytics import benchmark_discovery_miners, characterize_log, filter_top_k_variants, profile_bottlenecks
from src.loader import configure_graphviz, load_event_log
from src.visualize import export_bottleneck_dashboard, export_petri_net


def run_pipeline(
    input_path: Path,
    out_dir: Path,
    top_k: int = 10,
    min_transition_freq: int = 500,
    use_cache: bool = True,
) -> None:
    configure_graphviz()
    out_dir.mkdir(parents=True, exist_ok=True)

    t_total = time.perf_counter()

    # 1. Single-pass ingestion (with local pickle cache for iterative analysis)
    df, load_ms, source_label = load_event_log(input_path, use_cache=use_cache)
    print(f"\n=== Enterprise P2P Process Discovery Pipeline ===")
    print(f"Loaded {len(df):,} events from {source_label} in {load_ms:.1f} ms")

    # 2. Pre-mining log characterization on full unfiltered dataset
    t_char = time.perf_counter()
    stats = characterize_log(df)
    char_ms = (time.perf_counter() - t_char) * 1000.0

    print("\n=== Pre-Mining Log Characterization (Unfiltered ERP Log) ===")
    print(f"  Total Cases             : {stats['total_cases']:,}")
    print(f"  Total Events            : {stats['total_events']:,} (avg {stats['avg_events_per_case']} events/case)")
    print(f"  Unique Activities       : {stats['unique_activities']}")
    print(f"  Unique Trace Variants   : {stats['unique_variants']:,} (ratio: {stats['variant_to_case_ratio']:.4f})")
    print(f"  Variant Shannon Entropy : {stats['variant_entropy_bits']} bits (normalized: {stats['normalized_entropy']})")
    cov_str = " | ".join(f"Top-{k}: {pct:.2f}%" for k, pct in stats["top_k_coverage_pct"].items())
    print(f"  Cumulative Coverage     : {cov_str}")

    # 3. Filter top-k variants & benchmark 3 discovery algorithms
    filtered_df = filter_top_k_variants(df, top_k)
    filtered_cases = filtered_df["case:concept:name"].nunique()
    filtered_pct = (filtered_cases / stats["total_cases"] * 100.0) if stats["total_cases"] else 0.0
    print(f"\n=== Discovery Algorithm Benchmark (Top-{top_k} Variants: {filtered_cases:,} cases / {filtered_pct:.2f}%) ===")

    bench_rows, models = benchmark_discovery_miners(filtered_df)
    header = f"  {'Algorithm':<30} | {'Fitness':>7} | {'Prec.':>7} | {'F1':>7} | {'Simpl.':>7} | {'P/T/A':<11} | {'Disc. ms':>8}"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for r in bench_rows:
        pta = f"{r['places']}/{r['transitions']}/{r['arcs']}"
        print(
            f"  {r['algorithm']:<30} | {r['fitness']:>7.4f} | {r['precision']:>7.4f} | "
            f"{r['f1_score']:>7.4f} | {r['simplicity']:>7.4f} | {pta:<11} | {r['discover_ms']:>8.1f}"
        )

    # Export Petri net for the highest-F1 balanced model
    best_row = max(bench_rows, key=lambda x: x["f1_score"])
    best_net, best_im, best_fm = models[best_row["algorithm"]]
    petri_path = out_dir / "bpic_petri_net.png"
    export_petri_net(best_net, best_im, best_fm, petri_path)
    print(f"\n[+] Exported best-F1 Petri net ({best_row['algorithm']}) -> {petri_path}")

    # 4. Full-log bottleneck & cycle-time profiling
    t_perf = time.perf_counter()
    cycle_stats, top_bottlenecks, case_days = profile_bottlenecks(df, min_transition_freq=min_transition_freq)
    perf_ms = (time.perf_counter() - t_perf) * 1000.0

    print("\n=== End-to-End Cycle Time & Transition Bottlenecks (Full Log) ===")
    print(
        f"  Mean: {cycle_stats['mean_days']:.2f} d | Median: {cycle_stats['median_days']:.2f} d | "
        f"P90: {cycle_stats['p90_days']:.2f} d | P95: {cycle_stats['p95_days']:.2f} d"
    )
    print("\n  Top High-Volume Bottleneck Transitions:")
    for _, row in top_bottlenecks.iterrows():
        print(f"    {row['edge']:<58} | median: {row['median_days']:>6.2f} d | mean: {row['mean_days']:>6.2f} d | n={int(row['count']):,}")

    dashboard_path = out_dir / "p2p_bottleneck_profile.png"
    export_bottleneck_dashboard(case_days, top_bottlenecks, cycle_stats, dashboard_path)
    size_kb = dashboard_path.stat().st_size / 1024.0
    print(f"\n[+] Exported compact bottleneck dashboard -> {dashboard_path} ({size_kb:.1f} KB)")

    total_ms = (time.perf_counter() - t_total) * 1000.0
    print("\n=== Pipeline Timing Summary ===")
    print(f"  Log Load ({source_label:<24}) : {load_ms:8.1f} ms")
    print(f"  Pre-Mining Characterization          : {char_ms:8.1f} ms")
    print(f"  Bottleneck Profiling (Vectorized)    : {perf_ms:8.1f} ms")
    print(f"  Total End-to-End Runtime             : {total_ms:8.1f} ms\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Enterprise P2P Process Discovery & Bottleneck Pipeline.")
    parser.add_argument("--input", type=Path, default=Path("data/BPI_Challenge_2019.xes"))
    parser.add_argument("--out-dir", type=Path, default=Path("visualizations"))
    parser.add_argument("--top-k", type=int, default=10, help="Top-k trace variants for process discovery")
    parser.add_argument("--min-freq", type=int, default=500, help="Minimum transition count for bottleneck ranking")
    parser.add_argument("--no-cache", action="store_true", help="Bypass local .core.pkl cache and re-parse XES")
    args = parser.parse_args()

    if not args.input.exists():
        raise FileNotFoundError(
            f"Event log not found at {args.input}. Place BPI_Challenge_2019.xes in data/ or pass --input."
        )

    run_pipeline(
        input_path=args.input,
        out_dir=args.out_dir,
        top_k=args.top_k,
        min_transition_freq=args.min_freq,
        use_cache=not args.no_cache,
    )


if __name__ == "__main__":
    main()
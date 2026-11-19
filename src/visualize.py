from pathlib import Path
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import pm4py


def export_petri_net(net, initial_marking, final_marking, output_path: Path) -> None:
    """Render discovered Petri net to PNG via Graphviz."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    pm4py.save_vis_petri_net(net, initial_marking, final_marking, str(output_path))


def _shorten_label(text: str, max_len: int = 44) -> str:
    return text if len(text) <= max_len else text[: max_len - 3] + "..."


def export_bottleneck_dashboard(
    case_days: pd.Series,
    top_bottlenecks: pd.DataFrame,
    cycle_stats: dict[str, float],
    output_path: Path,
) -> None:
    """
    Generate a compact 2-panel performance visualization (<200 KB):
      - Left: End-to-end P2P cycle-time distribution (clipped at P95 to avoid outlier compression)
      - Right: Highest-friction activity transitions ranked by median wait time
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2), dpi=140)
    fig.patch.set_facecolor("#ffffff")

    # Panel 1: Cycle-Time Distribution (up to P95)
    p95 = max(float(cycle_stats.get("p95_days", 0.0)), 1.0)
    clipped = case_days[(case_days >= 0) & (case_days <= p95)]

    ax1.hist(clipped, bins=35, color="#2563eb", alpha=0.82, edgecolor="#ffffff", linewidth=0.6)
    ax1.axvline(cycle_stats["median_days"], color="#0f172a", linestyle="--", linewidth=1.5,
                label=f"Median: {cycle_stats['median_days']:.1f} d")
    ax1.axvline(cycle_stats["p90_days"], color="#dc2626", linestyle=":", linewidth=1.6,
                label=f"P90: {cycle_stats['p90_days']:.1f} d")
    ax1.set_title("End-to-End P2P Cycle Time (0 to P95)", fontsize=11, fontweight="bold", pad=10)
    ax1.set_xlabel("Case Duration (Days)", fontsize=9.5)
    ax1.set_ylabel("Number of Purchase Orders", fontsize=9.5)
    ax1.grid(axis="y", linestyle="--", alpha=0.35)
    ax1.spines["top"].set_visible(False)
    ax1.spines["right"].set_visible(False)
    ax1.legend(frameon=True, facecolor="#f8fafc", fontsize=8.5)

    # Panel 2: Top Bottleneck Activity Transitions
    plot_df = top_bottlenecks.iloc[::-1].copy()
    labels = [_shorten_label(e) for e in plot_df["edge"]]
    bars = ax2.barh(labels, plot_df["median_days"], color="#ea580c", alpha=0.85, height=0.62)

    max_wait = float(plot_df["median_days"].max()) if not plot_df.empty else 1.0
    ax2.set_xlim(0, max(max_wait * 1.32, 1.0))
    for bar, (_, row) in zip(bars, plot_df.iterrows()):
        ax2.text(
            bar.get_width() + max_wait * 0.02,
            bar.get_y() + bar.get_height() / 2.0,
            f"{row['median_days']:.1f}d (n={int(row['count']):,})",
            va="center",
            fontsize=8,
            color="#334155",
        )

    ax2.set_title("Top Bottleneck Transitions (Median Wait)", fontsize=11, fontweight="bold", pad=10)
    ax2.set_xlabel("Median Waiting Time (Days)", fontsize=9.5)
    ax2.tick_params(axis="y", labelsize=8)
    ax2.grid(axis="x", linestyle="--", alpha=0.35)
    ax2.spines["top"].set_visible(False)
    ax2.spines["right"].set_visible(False)

    plt.tight_layout(pad=2.0)
    fig.savefig(output_path, bbox_inches="tight", dpi=140)
    plt.close(fig)
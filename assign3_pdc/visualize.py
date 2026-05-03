"""Benchmark + worker-metric plotting (reads ``metrics.csv`` / ``benchmark_runs.csv``)."""

from __future__ import annotations

from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from metrics import (
    CrawlMetrics,
    MetricsCollector,
    build_example_benchmark_csv,
    metrics_to_dataframe,
)


def plot_run_comparison(
    runs: list[CrawlMetrics],
    *,
    outfile: str | Path | None = None,
    title: str = "Parallel crawler strategies",
    figsize: tuple[float, float] = (10, 5),
) -> None:
    """Bar chart duration and throughput across strategies."""
    if not runs:
        raise ValueError("runs must be non-empty")

    sns.set_theme(style="whitegrid", context="talk")
    df = metrics_to_dataframe(runs).sort_values("duration_sec")

    fig, axes = plt.subplots(1, 2, figsize=figsize)

    sns.barplot(data=df, x="strategy", y="duration_sec", ax=axes[0], palette="muted")
    axes[0].set_title("Wall time (s)")
    axes[0].set_ylabel("Seconds")
    axes[0].set_xlabel("Strategy")

    sns.barplot(
        data=df, x="strategy", y="throughput_pages_per_sec", ax=axes[1], palette="muted"
    )
    axes[1].set_title("Throughput (pages / s)")
    axes[1].set_ylabel("Pages per second")
    axes[1].set_xlabel("Strategy")

    fig.suptitle(title)
    fig.tight_layout()

    if outfile:
        fig.savefig(Path(outfile), dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_errors(runs: list[CrawlMetrics], *, outfile: str | Path | None = None) -> None:
    if not runs:
        return

    sns.set_theme(style="ticks", context="talk")
    df = metrics_to_dataframe(runs)
    fig, ax = plt.subplots(figsize=(8, 4))
    sns.barplot(data=df, x="strategy", y="errors", ax=ax, palette="rocket")
    ax.set_title("HTTP / transport errors observed")
    ax.set_xlabel("Strategy")
    fig.tight_layout()

    if outfile:
        fig.savefig(Path(outfile), dpi=150, bbox_inches="tight")
    plt.close(fig)


def _load_benchmark(path: Path) -> pd.DataFrame:
    cols = {"crawler", "num_workers", "execution_time_sec", "pages_per_second"}
    df = pd.read_csv(path)
    df.columns = [str(c).strip().lower() for c in df.columns]
    missing = cols - set(df.columns)
    if missing:
        raise ValueError(f"benchmark file missing columns: {missing}")
    return df


def _load_worker_metrics(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    df.columns = [c.strip().lower() for c in df.columns]
    need = {"timestamp", "worker_id", "pages_done"}
    missing = need - set(df.columns)
    if missing:
        raise ValueError(f"metrics file missing columns: {missing}")
    return df[["timestamp", "worker_id", "pages_done"]].copy()


def plot_execution_vs_workers(df: pd.DataFrame, outfile: Path) -> None:
    engines = df[df["crawler"].isin(["threaded", "multiprocessing", "ray"])].copy()
    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(
        data=engines,
        x="num_workers",
        y="execution_time_sec",
        hue="crawler",
        marker="o",
        dashes=False,
        ax=ax,
    )
    ax.set_title("Execution time versus worker count")
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Execution time (seconds)")
    ax.legend(title="Crawler")
    ax.set_xticks(sorted(engines["num_workers"].unique()))
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_speedup_vs_workers(df: pd.DataFrame, outfile: Path) -> None:
    frames: list[pd.DataFrame] = []
    crawlers = ["threaded", "multiprocessing", "ray"]

    for name in crawlers:
        blk = df[df["crawler"] == name].sort_values("num_workers")
        if blk.empty:
            continue
        t1 = blk.loc[blk["num_workers"] == 1, "execution_time_sec"]
        if t1.empty:
            continue
        base = float(t1.iloc[0])
        spd = blk.assign(
            speedup=base / blk["execution_time_sec"],
            crawler=name,
        )
        frames.append(spd)

    if not frames:
        return

    speed_df = pd.concat(frames, ignore_index=True)

    fig, ax = plt.subplots(figsize=(9, 5))
    sns.lineplot(
        data=speed_df,
        x="num_workers",
        y="speedup",
        hue="crawler",
        marker="o",
        dashes=False,
        ax=ax,
    )

    ideal_x = sorted(speed_df["num_workers"].unique())
    ideal_y = ideal_x  # Ideal linear speedup relative to the 1-worker run
    ax.plot(ideal_x, ideal_y, color="gray", linestyle="--", linewidth=2, label="Ideal speedup")
    ax.set_title("Speedup versus worker count (baseline = single worker)")
    ax.set_xlabel("Number of workers")
    ax.set_ylabel("Speedup")
    ax.legend(title=None)
    ax.set_xticks(ideal_x)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_pages_per_second_four(df: pd.DataFrame, outfile: Path) -> None:
    threaded = df[(df["crawler"] == "threaded") & (df["num_workers"] == 8)]
    multi = df[(df["crawler"] == "multiprocessing") & (df["num_workers"] == 8)]
    ray_rows = df[(df["crawler"] == "ray") & (df["num_workers"] == 8)]
    seq_rows = df[df["crawler"] == "sequential"]

    parts: list[dict[str, str | float]] = []
    if not seq_rows.empty:
        parts.append(
            {"config": "Sequential", "pps": float(seq_rows.iloc[0]["pages_per_second"])}
        )
    if not threaded.empty:
        parts.append(
            {
                "config": "Threaded (8)",
                "pps": float(threaded.iloc[0]["pages_per_second"]),
            }
        )
    if not multi.empty:
        parts.append(
            {
                "config": "Multiproc (8)",
                "pps": float(multi.iloc[0]["pages_per_second"]),
            }
        )
    if not ray_rows.empty:
        parts.append({"config": "Ray (8)", "pps": float(ray_rows.iloc[0]["pages_per_second"])})

    if len(parts) < 4:
        print(
            "[visualize] Missing some benchmark rows needed for plot 3; "
            f"supplying partial chart with {len(parts)} bars.",
        )

    bar_df = pd.DataFrame(parts)
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(
        data=bar_df,
        x="config",
        y="pps",
        hue="config",
        dodge=False,
        legend=False,
        palette="muted",
        ax=ax,
    )
    ax.set_title("Pages processed per second (100-page crawl target)")
    ax.set_xlabel("Configuration")
    ax.set_ylabel("Pages / second")
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)


def plot_utilization_heatmap(
    dfm: pd.DataFrame,
    *,
    outfile: Path,
    total_pages: int,
    bucket_secs: float = 0.5,
) -> None:
    dfm = dfm.copy()
    dfm["timestamp"] = pd.to_numeric(dfm["timestamp"], errors="coerce")
    dfm = dfm.dropna(subset=["timestamp"])
    dfm["worker_id"] = dfm["worker_id"].astype(int)
    dfm["pages_done"] = dfm["pages_done"].astype(int)

    t0 = dfm["timestamp"].min()
    dfm["time_bucket"] = ((dfm["timestamp"] - t0) // max(bucket_secs, 1e-6)).astype(int)

    grp = dfm.groupby(["worker_id", "time_bucket"])["pages_done"].max().unstack(fill_value=None)
    util = grp.sort_index(axis=1).astype(float)

    util = util.ffill(axis=1).fillna(0.0)

    denom = max(int(total_pages), 1)
    util_pct = (util / float(denom) * 100.0).clip(upper=100.0)

    fig_height = max(4.0, 0.5 * util_pct.shape[0])
    fig, ax = plt.subplots(figsize=(10, fig_height))
    sns.heatmap(
        util_pct,
        cmap="viridis",
        ax=ax,
        cbar_kws={"label": "Utilization (%)"},
    )
    ax.set_title("Approximate cumulative worker utilization by time slice")
    ax.set_xlabel("Time bucket index")
    ax.set_ylabel("Worker ID")
    fig.tight_layout()
    fig.savefig(outfile, dpi=150, bbox_inches="tight")
    plt.close(fig)


def generate_all_plots(
    *,
    metrics_path: str | Path = "metrics.csv",
    benchmark_path: str | Path = "benchmark_runs.csv",
    output_dir: str | Path = ".",
    total_pages_for_heatmap: int = 100,
    heatmap_bucket_sec: float = 0.5,
) -> None:
    """
    Render the four coursework figures described in ``assign3`` instructions.

    * ``metrics.csv`` should contain ``timestamp, worker_id, pages_done``.
    * ``benchmark_runs.csv`` should contain ``crawler, num_workers, execution_time_sec, pages_per_second``.
      A synthetic file is emitted automatically whenever the CSV is absent.
    """
    sns.set_theme(style="whitegrid")

    metrics_path = Path(metrics_path)
    benchmark_path = Path(benchmark_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not benchmark_path.exists():
        print(f"[visualize] Missing {benchmark_path}, writing illustrative benchmark table.")
        build_example_benchmark_csv(str(benchmark_path))

    bench_df = _load_benchmark(benchmark_path)

    plot_execution_vs_workers(bench_df, output_dir / "plot1_execution_time_vs_workers.png")
    plot_speedup_vs_workers(bench_df, output_dir / "plot2_speedup_vs_workers.png")
    plot_pages_per_second_four(bench_df, output_dir / "plot3_pages_per_second_four_configs.png")

    if metrics_path.exists():
        try:
            mdf = _load_worker_metrics(metrics_path)
            if mdf.empty:
                raise ValueError("metrics.csv is empty.")
            plot_utilization_heatmap(
                mdf,
                outfile=output_dir / "plot4_worker_utilization_heatmap.png",
                total_pages=total_pages_for_heatmap,
                bucket_secs=heatmap_bucket_sec,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"[visualize] Skipped heatmap: {exc!r}")
    else:
        print(f"[visualize] {metrics_path} not found — generating placeholder heatmap from demo samples.")
        demo = _demo_worker_metrics(total_pages_for_heatmap)

        demo_path = output_dir / "metrics_demo_placeholder.csv"
        demo.to_csv(demo_path, index=False)
        plot_utilization_heatmap(
            demo,
            outfile=output_dir / "plot4_worker_utilization_heatmap.png",
            total_pages=total_pages_for_heatmap,
            bucket_secs=heatmap_bucket_sec,
        )


def _demo_worker_metrics(total_pages: int) -> pd.DataFrame:
    """Synthetic trajectory used only when ``metrics.csv`` is absent."""

    collector = MetricsCollector(total_pages)
    collector.record(0.0, 0, 6)
    collector.record(0.3, 1, 4)
    collector.record(0.6, 0, 12)
    collector.record(0.65, 1, 13)
    collector.record(1.2, 0, 31)
    collector.record(1.25, 1, 38)
    collector.record(2.0, 0, total_pages // 2)
    collector.record(2.05, 1, total_pages // 2 + 11)
    return collector.to_dataframe()[["timestamp", "worker_id", "pages_done"]]


if __name__ == "__main__":
    generate_all_plots()

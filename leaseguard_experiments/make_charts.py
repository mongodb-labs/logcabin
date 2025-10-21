import argparse
import logging
import os.path

import matplotlib.font_manager as font_manager
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker


_logger = logging.getLogger("chart")
_this_dir = os.path.dirname(__file__)


def chart_network_latency(args: argparse.Namespace):
    csv = pd.read_csv(f"{_this_dir}/network_latency_experiment.csv")
    fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, sharex=True, figsize=(5, 6))

    ax4.set(xlabel="added one-way network latency (ms)")

    ax1.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax2.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax2.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, pos: f'{int(x/1000)}k'))
    ax3.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax4.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))

    ax1.xaxis.set_major_locator(plt.NullLocator())
    ax2.xaxis.set_major_locator(plt.NullLocator())
    ax3.xaxis.set_major_locator(plt.NullLocator())
    ax4.xaxis.set_major_locator(plt.MultipleLocator(1))

    ax1.set_ylim(0, 24)
    ax3.set_ylim(0, 24)
    ax4.set_ylim(0, 24)
    ax1.yaxis.set_major_locator(plt.MultipleLocator(10))
    ax3.yaxis.set_major_locator(plt.MultipleLocator(10))
    ax4.yaxis.set_major_locator(plt.MultipleLocator(10))
    
    for ax in [ax1, ax2, ax3, ax4]:
        ax.yaxis.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_axisbelow(True)

    # x-offset, color, config_name, operation_type, axes
    combos = [
        (-0.25, "C1", "inconsistent", "write", ax1),
        (0.25, "C0", "inconsistent", "read", ax1),
        (-0.25, "C1", "quorum", "write", ax2),
        (0.25, "C0", "quorum", "read", ax2),
        (-0.25, "C1", "Ongaro lease", "write", ax3),
        (0.25, "C0", "Ongaro lease", "read", ax3),
        (-0.25, "C1", "LeaseGuard", "write", ax4),
        (0.25, "C0", "LeaseGuard", "read", ax4),
    ]

    for offset, color, config_name, operationType, ax in combos:
        if config_name == "inconsistent":
            config_predicate = (csv["quorumCheckOnRead"] == False) & (
                csv["leaseGuardEnabled"] == False
            ) & (
                csv["ongaroLeaseEnabled"] == False
            )
        elif config_name == "quorum":
            config_predicate = csv["quorumCheckOnRead"]
        elif config_name == "Ongaro lease":
            config_predicate = csv["ongaroLeaseEnabled"]
        else:
            config_predicate = csv["leaseGuardEnabled"]

        op_predicate = csv["operationType"] == operationType
        column = "p90latencyNanos"
        df = (
            csv[config_predicate & op_predicate]
            .groupby(
                [
                    "latencyMs",
                    "operationType",
                    "quorumCheckOnRead",
                    "ongaroLeaseEnabled",
                    "leaseGuardEnabled",
                    "deferCommitEnabled",
                    "inheritLeaseEnabled",
                    "size",
                    "threads",
                    "operations",
                ]
            )[column]
            .mean()
            .reset_index()
        )

        hatch = "//" if operationType == "write" else "xx"

        # The x-axis "latencyMs" is the artificially added network latency.
        x = df["latencyMs"] + offset
        # convert nanos to millis and ensure min height of 1
        y = df[column].apply(lambda x: max(x / 1_000_000, 1))
        # Draw hatch only.
        ax.bar(
            x,
            y,
            label=f"{config_name} {operationType}",
            color="none",
            width=0.3,
            edgecolor=color,
            hatch=hatch,
            facecolor="none",
            linewidth=0.5,
            zorder=2,
        )
        # Draw the edge.
        ax.bar(
            x,
            y,
            color="none",
            width=0.3,
            edgecolor="black",
            facecolor="none",
            linewidth=0.5,
            zorder=3,
        )
        # Add config_name to the upper left interior of each subplot
        ax.text(
            0.02, 0.7, config_name,
            transform=ax.transAxes,
            verticalalignment='top',
            horizontalalignment='left',
            fontsize=12,
        )

    # Draw hatch only.
    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[
            Patch(
                facecolor="none",
                edgecolor="C1",
                label="write latency p90",
                hatch="//",
                linewidth=0.5,
            ),
            Patch(
                facecolor="none",
                edgecolor="C0",
                label="read latency p90",
                hatch="xx",
                linewidth=0.5,
            ),
        ],
        frameon=False,
    )
    # Draw edges.
    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[
            Patch(
                facecolor="none",
                edgecolor="black",
                label="write latency p90",
                linewidth=0.5,
            ),
            Patch(
                facecolor="none",
                edgecolor="black",
                label="read latency p90",
                linewidth=0.5,
            ),
        ],
        frameon=False,
        labelcolor="none",
    )

    fig.text(0.002, 0.5, "p90 latency (ms)", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    chart_path = f"{_this_dir}/network_latency_experiment_logcabin.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_unavailability(args: argparse.Namespace):
    from unavailability_experiment import (
        OPTIONS,
        ELECTION_TIMEOUT_MS,
        KILL_LEADER_TIME_MS,
        LEASE_TIMEOUT_MS,
        UnavailabilityBenchmarkOptions,
    )

    def resample_data(benchmark_index: int, options: UnavailabilityBenchmarkOptions):
        df = pd.read_csv(f"{_this_dir}/unavailability_experiment-{benchmark_index}.csv")
        for column in ["quorumCheckOnRead",
                       "ongaroLeaseEnabled",
                       "leaseGuardEnabled",
                       "deferCommitEnabled"]:
            assert df[column].nunique() == 1 and df[column].iloc[0] == getattr(
                options, column
            )

        interval = 1_000_000  # 1ms in nanos.
        df["time_bin"] = (df["stopNanos"] // interval) * interval
        df_resampled = (
            df.groupby(["time_bin", "operationType"])
            .size()
            .unstack(fill_value=0)
            .rename(columns={"read": "reads", "write": "writes"})
            .reset_index()
        )
        # Interpolate missing time_bin values
        all_time_bins = pd.DataFrame(
            {
                "time_bin": range(
                    df["time_bin"].min(), df["time_bin"].max() + interval, interval
                )
            }
        )
        df_resampled = all_time_bins.merge(
            df_resampled, on="time_bin", how="left"
        ).fillna(0)
        # Apply rolling average.
        df_resampled[["reads", "writes"]] = (
            df_resampled[["reads", "writes"]].rolling(window=30, min_periods=1).mean()
        )
        # Cut off the first and last data.
        start_time = df_resampled["time_bin"].min() + 20 * interval
        end_time = start_time + 2 * LEASE_TIMEOUT_MS * 1_000_000
        df_resampled = df_resampled[
            (df_resampled["time_bin"] >= start_time)
            & (df_resampled["time_bin"] <= end_time)
        ]
        return df_resampled

    dfs = {
        name: resample_data(i, options)
        for i, (name, options) in enumerate(OPTIONS.items())
    }
    fig, axes = plt.subplots(len(OPTIONS), 1, sharex=True, sharey=False, figsize=(5, 8))
    axes[-1].set(xlabel=r"time in milliseconds $\rightarrow$")
    label_font_size = 12

    for i, (name, df) in enumerate(dfs.items()):
        ax = axes[i]
        options = OPTIONS[name]
        x_min = df["time_bin"].min()
        y_lim = options.reads_per_ms * 2.1 * 1000  # ops/sec
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)

        for column in ["reads", "writes"]:
            ax.plot(
                (df["time_bin"] - x_min) / 1_000_000,
                df[column] * 1000,  # Convert ops/ms to ops/second
                label=column,
                linewidth=1,
            )
            ax.set_ylim(0, y_lim)
            ax.set_xlim(100, 1900)

        # Defer commit lets write throughput spike off the chart when old lease expires.
        if options.deferCommitEnabled:
            # Get the first off-chart value
            off_chart_writes = df[df["writes"] * 1000 > y_lim]
            off_chart_value = off_chart_writes["writes"].iloc[0] * 1000
            off_chart_time = off_chart_writes.index[0]
            # Place text label just below the top of the subplot.
            ax.text(off_chart_time - 65, y_lim + 9500,
                    f"off chart, {off_chart_value/1000:.0f}k ops/sec",
                    ha="right", va="top",
                    fontsize=label_font_size)
            ax.text(off_chart_time - 75, y_lim + 6500,
                    r"$\searrow$",
                    ha="left", va="top")

        # Leader crash.
        ax.axvline(x=KILL_LEADER_TIME_MS, color="red", linestyle="dotted")
        # New leader elected.
        ax.axvline(
            x=KILL_LEADER_TIME_MS + ELECTION_TIMEOUT_MS,
            color="green",
            linestyle="dotted",
        )
        if options.ongaroLeaseEnabled or options.leaseGuardEnabled:
            # Old lease expires.
            ax.axvline(
                x=KILL_LEADER_TIME_MS + LEASE_TIMEOUT_MS,
                color="purple",
                linestyle="dotted",
            )

        ax.text(
            1.05,
            0.5,
            name,
            va="center",
            ha="center",
            rotation="vertical",
            transform=ax.transAxes,
        )

    EVENT_LABEL_HEIGHT = 20000
    axes[0].text(510,
                 EVENT_LABEL_HEIGHT,
                 "$\\leftarrow$ leader\n    crash",
                 color="red",
                 fontsize=label_font_size)
    axes[1].text(570,
                 EVENT_LABEL_HEIGHT / 10,  # the "quorum" chart's y-axis is short
                 "new leader\nelected    $\\rightarrow$",
                 color="green",
                 fontsize=label_font_size)
    axes[2].text(1090,
                 EVENT_LABEL_HEIGHT,
                 "old lease\nexpires  $\\rightarrow$ ",
                 color="purple",
                 fontsize=label_font_size)
    fig.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 0.98),
        ncol=2,
        handles=[Line2D([0], [0], color=color) for color in ["C1", "C0"]],
        labels=["writes", "reads"],
        frameon=False, # remove border
    )
    fig.text(0.002, 0.5, "operations per second", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = f"{_this_dir}/unavailability_experiment_logcabin.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_latency_vs_throughput(args: argparse.Namespace):
    """Plot latency (p50) vs throughput with one subplot per configuration.

    Each subplot corresponds to one configuration (the first five boolean
    options). Within a subplot we draw a separate line per write ratio.
    """
    csv = pd.read_csv(f"{_this_dir}/latency_vs_throughput_experiment.csv")
    csv["write_ratio"] = csv["writes"] / (csv["reads"] + csv["writes"])
    config_keys = (
        "quorumCheckOnRead",
        "ongaroLeaseEnabled",
        "leaseGuardEnabled",
        "deferCommitEnabled",
        "inheritLeaseEnabled",
    )

    names = {
        (False, False, False, False, False): "inconsistent",
        (True, False, False, False, False): "quorum",
        (False, True, False, False, False): "Ongaro lease",
        (False, False, True, True, True): "LeaseGuard",
    }

    # Mean per trial (grouping by config keys + reads/writes to pair read/write rows).
    mean_per_trial = (
        csv.groupby(list(config_keys) + ["write_ratio", "reads", "writes", "operationType"])
        [["opsPerSec", "p50latencyNanos"]].mean().reset_index())

    # Pivot read/write into columns.
    pivot = mean_per_trial.pivot_table(
        index=list(config_keys) + ["write_ratio", "reads", "writes"],
        columns="operationType",
        values=["opsPerSec", "p50latencyNanos"])
    pivot.columns = [f"{v}_{k}" for v, k in pivot.columns]
    pivot = pivot.reset_index()

    # Compute combined throughput and weighted p50 (ms)
    pivot["total_ops_per_sec"] = pivot["opsPerSec_read"] + pivot["opsPerSec_write"]
    pivot["combined_p50_ms"] = (
        (pivot["p50latencyNanos_read"] * pivot["opsPerSec_read"]
         + pivot["p50latencyNanos_write"] * pivot["opsPerSec_write"])
        / pivot["total_ops_per_sec"]
    ) / 1_000_000.0

    # Group by configurations (first five keys).
    grouped = dict(list(pivot.groupby(["write_ratio"] + list(config_keys))))
    for write_ratio in sorted(pivot["write_ratio"].unique()):
        fig, ax = plt.subplots(1, 1, sharex=True, figsize=(8, 3.5))
        # ax.set_yscale("log")
        # if write_ratio == 0.0:
        #     ax.set_ylim(0, 0.3)
        #     yticks = [0, 0.1, 0.2, 0.3]
        # else:
        #     ax.set_ylim(0, 10)
        #     yticks = range(0, 11, 2)
        # ax.set_yticks(list(yticks))
        # ax.set_yticklabels([str(v) for v in yticks])

        # Draw each configuration as a separate line on the same axes.
        for group_vars, config_name in names.items():
            key = (write_ratio,) + group_vars
            group_df = grouped[key]
            markers = {
                "inconsistent": "o",
                "quorum": "s",
                "Ongaro lease": "^",
                "LeaseGuard": "D",
            }
            marker = markers.get(config_name, "o")
            ax.plot(
                group_df["total_ops_per_sec"],
                group_df["combined_p50_ms"],
                linewidth=0.9,
                marker=marker,
                markersize=5,
                markeredgewidth=0.5,
                zorder=2,
                label=config_name,
            )
            if args.labels:
                for xi, yi in zip(group_df["total_ops_per_sec"], group_df["combined_p50_ms"]):
                    ax.annotate(
                        f"{int(round(xi))},{int(round(yi))}",
                        xy=(xi, yi),
                        xytext=(3, 3),
                        textcoords="offset points",
                        fontsize=8,
                        zorder=4,
                    )

        ax.yaxis.grid(True, which="both", linestyle="--", linewidth=0.5)
        ax.set_axisbelow(True)
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)

        ax.set(xlabel="actual throughput (ops/sec)")
        ax.set_ylabel("average latency (ms)")
        fig.suptitle(f"{int(write_ratio * 100)}% write ratio", y=0.99, fontsize=14)

        # Legend for configurations at the top
        handles, labels = ax.get_legend_handles_labels()
        if handles:
            fig.legend(handles, labels, loc="upper center", ncol=min(len(labels), 4), frameon=False)

        fig.tight_layout()
        fig.subplots_adjust(top=0.88)
        chart_path = f"{_this_dir}/latency_vs_throughput_experiment_logcabin_{write_ratio}.pdf"
        fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
        _logger.info(f"Created {chart_path}")

        # Export CSV containing all configurations for this write_ratio.
        csv_rows = []
        for group_vars, group_df in grouped.items():
            if group_vars[0] == write_ratio:
                csv_rows.append(group_df)
        if csv_rows:
            df_out = pd.concat(csv_rows, ignore_index=True)
            csv_path = chart_path.replace(".pdf", ".csv")
            df_out.to_csv(csv_path, index=False)
            _logger.info(f"Created {csv_path}")


if __name__ == "__main__":
    chart_funcs = {
        "network_latency": chart_network_latency,
        "unavailability": chart_unavailability,
        "latency_vs_throughput": chart_latency_vs_throughput,
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("charts", nargs="*", help=f"Which charts to make: {','.join(chart_funcs)}")
    parser.add_argument("--labels", 
                        action="store_true", help="Whether to add data labels to points.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    plt.rcParams.update({"font.size": 12})
    plt.rcParams["hatch.linewidth"] = 0.5
    font_path = (
        f"{_this_dir}/cmunrm.ttf"  # Computer Modern Roman, like Latex's default.
    )
    font_manager.fontManager.addfont(font_path)
    font_properties = font_manager.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_properties.get_name()

    for chart_name, chart_func in chart_funcs.items():
        if chart_name in args.charts or args.charts == []:
            chart_funcs[chart_name](args)

import argparse
import logging
import os.path
import fractions

import matplotlib.font_manager as font_manager
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import matplotlib.pyplot as plt
import pandas as pd


_logger = logging.getLogger("chart")
_this_dir = os.path.dirname(__file__)


def chart_network_latency():
    csv = pd.read_csv(f"{_this_dir}/network_latency_experiment.csv")
    fig, (ax1, ax2, ax3) = plt.subplots(3, 1, sharex=True, figsize=(5, 4))

    ax3.set(xlabel="added one-way network latency (ms)")

    ax1.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax2.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))
    ax3.yaxis.set_major_locator(plt.MaxNLocator(nbins=3))

    ax1.xaxis.set_major_locator(plt.NullLocator())
    ax2.xaxis.set_major_locator(plt.NullLocator())
    ax3.xaxis.set_major_locator(plt.MultipleLocator(1))

    for ax in [ax1, ax2, ax3]:
        ax.yaxis.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_axisbelow(True)

    # x-offset, color, config_name, operation_type, axes
    combos = [
        (-0.25, "C1", "inconsistent", "write", ax1),
        (0.25, "C0", "inconsistent", "read", ax1),
        (-0.25, "C1", "lease", "write", ax2),
        (0.25, "C0", "lease", "read", ax2),
        (-0.25, "C1", "quorum", "write", ax3),
        (0.25, "C0", "quorum", "read", ax3),
    ]

    for offset, color, config_name, operationType, ax in combos:
        if config_name == "inconsistent":
            config_predicate = (csv["quorumCheckOnRead"] == False) & (
                csv["leaseEnabled"] == False
            )
        elif config_name == "quorum":
            config_predicate = csv["quorumCheckOnRead"]
        else:
            config_predicate = (
                (csv["leaseEnabled"])
                & (csv["deferCommitEnabled"])
                & (csv["inheritLeaseEnabled"])
            )

        op_predicate = csv["operationType"] == operationType
        column = "p95latencyNanos"
        df = (
            csv[config_predicate & op_predicate]
            .groupby(
                [
                    "latencyMs",
                    "operationType",
                    "quorumCheckOnRead",
                    "leaseEnabled",
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
                facecolor="none", edgecolor="C1", label="write latency", hatch="//", linewidth=0.5
            ),
            Patch(
                facecolor="none", edgecolor="C0", label="read latency", hatch="xx", linewidth=0.5
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
                facecolor="none", edgecolor="black", label="write latency", linewidth=0.5
            ),
            Patch(
                facecolor="none", edgecolor="black", label="read latency", linewidth=0.5
            ),
        ],
        frameon=False,
        labelcolor="none",
    )
    
    fig.text(0.002, 0.5, "milliseconds", va="center", rotation="vertical")

    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    chart_path = f"{_this_dir}/network_latency_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


def chart_unavailability():
    from unavailability_experiment import (
        OPTIONS,
        ELECTION_TIMEOUT_MS,
        KILL_LEADER_TIME_MS,
        LEASE_TIMEOUT_MS,
        UnavailabilityBenchmarkOptions,
    )

    def resample_data(benchmark_index: int, options: UnavailabilityBenchmarkOptions):
        df = pd.read_csv(f"{_this_dir}/unavailability_experiment-{benchmark_index}.csv")
        for column in ["quorumCheckOnRead", "leaseEnabled", "deferCommitEnabled"]:
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
        end_time = start_time + 3 * LEASE_TIMEOUT_MS * 1_000_000
        df_resampled = df_resampled[
            (df_resampled["time_bin"] >= start_time)
            & (df_resampled["time_bin"] <= end_time)
        ]
        return df_resampled

    dfs = {
        name: resample_data(i, options)
        for i, (name, options) in enumerate(OPTIONS.items())
    }
    fig, axes = plt.subplots(len(OPTIONS), 1, sharex=True, sharey=False, figsize=(5, 5))
    axes[-1].set(xlabel=r"time in milliseconds $\rightarrow$")

    for i, (name, df) in enumerate(dfs.items()):
        ax = axes[i]
        options = OPTIONS[name]
        x_min = df["time_bin"].min()
        for spine in ax.spines.values():
            spine.set_linewidth(0.5)

        if len(df) > 0:
            for column in ["reads", "writes"]:
                ax.plot(
                    (df["time_bin"] - x_min) / 1_000_000,
                    df[column],
                    label=column,
                    linewidth=0.75,
                )
                ax.set_ylim(0, options.reads_per_ms * 2.5)

        # Leader crash.
        ax.axvline(x=KILL_LEADER_TIME_MS, color="red", linestyle="dotted")
        # New leader elected.
        ax.axvline(
            x=KILL_LEADER_TIME_MS + ELECTION_TIMEOUT_MS,
            color="green",
            linestyle="dotted",
        )
        if options.leaseEnabled:
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

    label_y_top = OPTIONS["inconsistent"].reads_per_ms * 2
    label_font_size = 10
    axes[0].text(
        KILL_LEADER_TIME_MS + 50,
        int(label_y_top) * 0.925,
        r"$\leftarrow$ leader crash",
        color="red",
        bbox=dict(facecolor="white", edgecolor="none"),
        fontsize=label_font_size,
    )
    axes[0].text(
        KILL_LEADER_TIME_MS + ELECTION_TIMEOUT_MS + 50,
        int(label_y_top * 0.7),
        r"$\leftarrow$ new leader elected",
        color="green",
        fontsize=label_font_size,
    )
    axes[2].text(
        KILL_LEADER_TIME_MS + LEASE_TIMEOUT_MS - 50,
        int(label_y_top * 0.75),
        r"old lease expires $\rightarrow$",
        color="purple",
        bbox=dict(facecolor="white", edgecolor="none"),
        horizontalalignment="right",
        fontsize=label_font_size,
    )
    fig.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.005),
        ncol=2,
        handles=[Line2D([0], [0], color=color) for color in ["C1", "C0"]],
        labels=["writes", "reads"],
        frameon=False, # remove border
    )
    fig.text(0.002, 0.5, "operations per millisecond", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = f"{_this_dir}/unavailability_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    chart_funcs = {
        "network_latency": chart_network_latency,
        "unavailability": chart_unavailability,
    }

    parser = argparse.ArgumentParser()
    parser.add_argument("charts", nargs="*", help=f"Which charts to make: {','.join(chart_funcs)}")
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
            chart_funcs[chart_name]()

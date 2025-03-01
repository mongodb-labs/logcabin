import argparse
import logging
import os.path
import fractions

import matplotlib.font_manager as font_manager
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Patch

from lib import BenchmarkOptions


_logger = logging.getLogger("chart")
_this_dir = os.path.dirname(__file__)


def chart_network_latency():
    csv = pd.read_csv(f"{_this_dir}/network_latency_experiment.csv")
    LAT_RANGE = csv["latencyMs"].max() - csv["latencyMs"].min()
    LAT_CARDINALITY = csv["latencyMs"].unique().size
    LAT_INTERVAL = LAT_RANGE / (LAT_CARDINALITY - 1)
    # Room for 6 bars for each configuration plus some padding.
    BARWIDTH = LAT_INTERVAL / 8
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.set(xlabel="added one-way network latency (ms)")
    ax.tick_params(axis="x", bottom=False)
    ax.set_yscale("log")  # Set y-axis to logarithmic scale

    def format_func(y, _):
        if y < 1:
            return str(fractions.Fraction(y).limit_denominator())
        return f"{int(y)}"

    ax.yaxis.set_major_formatter(plt.FuncFormatter(format_func))
    ax.yaxis.set_major_locator(plt.LogLocator(base=10))
    ax.yaxis.set_minor_locator(plt.NullLocator())  # Remove minor ticks
    
    ax.xaxis.set_major_locator(plt.MultipleLocator(1))

    # x-offset, color, config_name, operation_type
    combos = [
        (-2.5, "C1", "inconsistent", "write"),
        (-1.5, "C0", "inconsistent", "read"),
        (-0.5, "C1", "lease", "write"),
        (0.5, "C0", "lease", "read"),
        (1.5, "C1", "quorum", "write"),
        (2.5, "C0", "quorum", "read"),
    ]

    for offset, color, config_name, operationType in combos:
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

        # The x-axis "latencyMs" is the artificially added network latency.
        ax.bar(
            df["latencyMs"] + offset * BARWIDTH,
            df[column] / 1_000_000,  # convert nanos to millis
            BARWIDTH,
            label=column,
            color=color,
        )

    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[Patch(color=color) for color in ["C1", "C0"]],
        handleheight=0.65,
        handlelength=0.65,
        labels=["write latency p90", "read latency p90"],
        frameon=False,
    )
    arrow_x = csv["latencyMs"].min()
    arrow_y = csv[csv["latencyMs"] == 1][column].max() / 1_000_000

    for i in range(0, len(combos), 2):
        offset, color, config_name, operationType = combos[i]
        ax.text(
            arrow_x + 0.1 + 1.3 * offset * BARWIDTH,
            arrow_y + 0.2,
            rf"$\leftarrow$ {config_name}",
            horizontalalignment="center",
            verticalalignment="bottom",
            rotation="vertical",
            fontdict={"fontsize": 11},
        )

    fig.text(0.002, 0.55, "milliseconds (log scale)", va="center", rotation="vertical")

    # Remove chart borders
    for spine in ax.spines.values():
        spine.set_visible(False)

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
    font_path = (
        f"{_this_dir}/cmunrm.ttf"  # Computer Modern Roman, like Latex's default.
    )
    font_manager.fontManager.addfont(font_path)
    font_properties = font_manager.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_properties.get_name()

    for chart_name, chart_func in chart_funcs.items():
        if chart_name in args.charts or args.charts == []:
            chart_funcs[chart_name]()

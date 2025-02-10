import argparse
import logging
import os.path

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
    BARWIDTH = 0.15
    LINEWIDTH = 0.01
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.set(xlabel="one-way network latency (µs)")
    ax.tick_params(axis="x", bottom=False)
    ax.set_yscale("log")  # Set y-axis to logarithmic scale
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{int(y)}'))  # Use whole numbers

    # x-offset, color, config_name
    combos = [
        (-1.5, "C0", "inconsistent"),
        (0, "C0", "lease"),
        (1.5, "C0", "quorum"),
    ]

    for offset, color, config_name in combos:
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

        column = "p95latencyNanos"
        df = (
            csv[config_predicate]
            .groupby(
                [
                    "latencyMs",
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
            df["latencyMs"] + offset * (BARWIDTH + LINEWIDTH * 2),
            df[column] / 1_000_000,  # convert nanos to millis
            BARWIDTH,
            label=column,
            color=color,
            edgecolor=color,
            linewidth=LINEWIDTH,
        )

    fig.legend(
        loc="upper center",
        ncol=2,
        handles=[Patch(color="C0")],
        handleheight=0.65,
        handlelength=0.65,
        labels=["read latency p95"],
        frameon=False,
    )
    arrow_x = csv["latencyMs"].min()
    arrow_y = csv[csv["latencyMs"] == 0][column].max() / 1_000_000

    for i in range(len(combos)):
        offset, color, config_name = combos[i]
        ax.text(
            arrow_x + (offset - 0.5) * (BARWIDTH + 2 * LINEWIDTH),
            arrow_y + 0.2,
            rf"$\leftarrow$ {config_name}",
            horizontalalignment="left",
            verticalalignment="bottom",
            rotation="vertical",
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
    )

    def resample_data(benchmark_index: int, options: BenchmarkOptions):
        df = pd.read_csv(f"{_this_dir}/unavailability_experiment-{benchmark_index}.csv")
        for column in ["quorumCheckOnRead", "leaseEnabled", "deferCommitEnabled"]:
            assert df[column].nunique() == 1 and df[column].iloc[0] == getattr(options, column)
        
        interval = 10_000_000  # 10ms in nanos.
        df["time_bin"] = (df["recordedAtNanos"] // interval) * interval
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
        return df_resampled

    dfs = {name: resample_data(i, options) for i, (name, options) in enumerate(OPTIONS.items())}
    y_lim = 1.1 * max(df["reads"].max() for df in dfs.values())
    fig, axes = plt.subplots(len(OPTIONS), 1, sharex=True, sharey=True, figsize=(5, 5))
    axes[-1].set(xlabel=r"time in milliseconds $\rightarrow$")

    for i, (name, df) in enumerate(dfs.items()):
        ax = axes[i]
        options = OPTIONS[name]
        x_min = df["time_bin"].min()
        # Remove borders
        for spine in ax.spines.values():
            spine.set_visible(False)

        if len(df) > 0:
            for column in ["reads", "writes"]:
                ax.plot(
                    (df["time_bin"] - x_min) / 1_000_000,
                    df[column],
                    label=column,
                )
                ax.set_ylim(0, y_lim)

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
            1.02,
            0.5,
            name,
            va="center",
            ha="center",
            rotation="vertical",
            transform=ax.transAxes,
        )

    axes[0].text(
        KILL_LEADER_TIME_MS + 50,
        int(y_lim * 0.85),
        r"$\leftarrow$ leader crash",
        color="red",
        bbox=dict(facecolor="white", edgecolor="none"),
    )
    axes[0].text(
        KILL_LEADER_TIME_MS + ELECTION_TIMEOUT_MS + 50,
        int(y_lim * 0.6),
        r"$\leftarrow$ new leader elected",
        color="green",
    )
    axes[2].text(
        KILL_LEADER_TIME_MS + LEASE_TIMEOUT_MS + 50,
        int(y_lim * 0.75),
        r"$\leftarrow$ old lease expires",
        color="purple",
        bbox=dict(facecolor="white", edgecolor="none"),
    )
    fig.legend(
        loc="upper center",
        bbox_to_anchor=(0.5, 1.005),
        ncol=2,
        handles=[Line2D([0], [0], color=color) for color in ["C1", "C0"]],
        labels=["writes", "reads"],
    )
    fig.text(0.002, 0.5, "operations per millisecond", va="center", rotation="vertical")
    fig.tight_layout()
    fig.subplots_adjust(hspace=0.4, top=0.92)
    chart_path = f"{_this_dir}/unavailability_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("charts", nargs="*", help="Which charts to make")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    plt.rcParams.update({"font.size": 12})
    font_path = f"{_this_dir}/cmunrm.ttf"  # Computer Modern Roman, like Latex's default.
    font_manager.fontManager.addfont(font_path)
    font_properties = font_manager.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_properties.get_name()

    chart_funcs = {
        "network_latency": chart_network_latency,
        "unavailability": chart_unavailability,
    }

    for chart_name, chart_func in chart_funcs.items():
        if chart_name in args.charts or args.charts == []:
            chart_funcs[chart_name]()

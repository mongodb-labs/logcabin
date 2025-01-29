import logging

import matplotlib.font_manager as font_manager
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.patches import Patch

_logger = logging.getLogger("chart")


def chart_network_latency():
    csv = pd.read_csv("network_latency_experiment.csv")
    BARWIDTH = .09
    LINEWIDTH = .01
    fig, ax = plt.subplots(figsize=(5, 3))
    ax.set(xlabel="one-way network latency (µs)")
    ax.tick_params(axis="x", bottom=False)
    
    # x-offset, color, config_name, operationType
    combos = [
        (-2.4, "C1", "inconsistent", "write"),
        (-1.4, "C0", "inconsistent", "read"),
        (0, "C1", "lease", "write"),
        (1, "C0", "lease", "read"),
        (2.4, "C1", "quorum", "write"),
        (3.4, "C0", "quorum", "read"),
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
        column = "p90latencyMicros"
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

        # "latencyMs" is the artificially added network latency.
        ax.bar(
            df["latencyMs"] + offset * (BARWIDTH + LINEWIDTH * 2),
            df[column] / 1000,  # convert micros to millis
            BARWIDTH,
            label=column,
            color=color,
            edgecolor=color,
            linewidth=LINEWIDTH,
        )

    fig.legend(
        loc="upper center",
        bbox_to_anchor=(0.59, 1.03),
        ncol=2,
        handles=[Patch(color=color) for color in ["C1", "C0"]],
        handleheight=0.65,
        handlelength=0.65,
        labels=["write latency p90", "read latency p90"],
        frameon=False,
    )
    arrow_x = csv["latencyMs"].min()
    arrow_y = csv[csv["latencyMs"] == 0]["p90latencyMicros"].max() / 1000
    
    for i in range(0, len(combos), 2):
        offset, color, config_name, operationType = combos[i]
        ax.text(
            arrow_x + (offset - .5) * (BARWIDTH + 2 * LINEWIDTH),
            arrow_y + 2,
            rf"$\leftarrow$ {config_name}",
            horizontalalignment="left",
            verticalalignment="bottom",
            rotation="vertical",
        )

    fig.text(0.002, 0.55, "milliseconds", va="center", rotation="vertical")

    # Remove chart borders
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    chart_path = "network_latency_experiment.pdf"
    fig.savefig(chart_path, bbox_inches="tight", pad_inches=0)
    _logger.info(f"Created {chart_path}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    plt.rcParams.update({"font.size": 12})
    font_path = "cmunrm.ttf"  # Computer Modern Roman, like Latex's default.
    font_manager.fontManager.addfont(font_path)
    font_properties = font_manager.FontProperties(fname=font_path)
    plt.rcParams["font.family"] = font_properties.get_name()
    chart_network_latency()

"""Test the effect of network latency, with or without leases."""

import argparse
import csv
import os
import time
from dataclasses import asdict, dataclass
from datetime import datetime

from lib import (
    BenchmarkOptions,
    BenchmarkResult,
    dataclass_fieldnames,
    dataclass_from_row,
    run_command,
    run_ssh_command,
    title,
    write_config_files
)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--servers", type=str, required=True, help="Comma-separated list of addresses"
)
parser.add_argument(
    "--trials", type=int, default=5, help="Number of trials for each config"
)
args = parser.parse_args()
SERVERS = args.servers.split(",")


class Stats:
    CSV_FILE_PATH = os.path.dirname(__file__) + "/network_latency_experiment.csv"

    @dataclass
    class Row:
        options: BenchmarkOptions
        result: BenchmarkResult

    def __init__(self):
        self.rows: list[Stats.Row] = []

    def load(self):
        if os.path.exists(Stats.CSV_FILE_PATH):
            with open(Stats.CSV_FILE_PATH, mode="r") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise ValueError("CSV file has no columns.")

                for row in reader:
                    self.rows.append(
                        Stats.Row(
                            options=dataclass_from_row(BenchmarkOptions, row),
                            result=dataclass_from_row(BenchmarkResult, row),
                        )
                    )

    def append(self, options: BenchmarkOptions, result: BenchmarkResult):
        self.rows.append(Stats.Row(options=options, result=result))

    def save(self):
        with open(Stats.CSV_FILE_PATH, mode="w") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=(
                    dataclass_fieldnames(BenchmarkOptions)
                    + dataclass_fieldnames(BenchmarkResult)
                ),
            )
            writer.writeheader()
            writer.writerows(asdict(row.options) | asdict(row.result) for row in self.rows)


def run_benchmark(options: BenchmarkOptions, stats: Stats):
    write_config_files(SERVERS, options)

    for server_id, addr in enumerate(SERVERS, start=1):
        title(f"SETUP {addr}")
        run_ssh_command(
            addr,
            f"""
        sudo tc qdisc del dev ens5 root > /dev/null 2>&1 || true # cleanup past rules
        cd logcabin
        killall -q -9 perf LogCabin Reconfigure || true
        rm -rf /tmp/logcabin {server_id}.log
        """,
        )

        if server_id == 1:
            run_ssh_command(
                addr, "cd logcabin; ./build/LogCabin --config conf1.conf --bootstrap"
            )

        run_ssh_command(
            addr,
            f"""
            cd logcabin
            nohup ./build/LogCabin --config conf{server_id}.conf --log {server_id}.log >{server_id}.out 2>&1 </dev/null &
            ps aux | grep LogCabin""",
        )

    time.sleep(5)

    title("RECONFIGURE")
    run_command(
        f"./build/Examples/Reconfigure --cluster={SERVERS[0]} set {' '.join(SERVERS)}"
    )

    title("HELLOWORLD")
    run_command(f"./build/Examples/HelloWorld --cluster={','.join(SERVERS)}")

    for server_id, addr in enumerate(SERVERS, start=1):
        title(f"CONFIG NETWORK {addr}")
        run_ssh_command(
            addr,
            f"""
        sudo iptables -t mangle -A OUTPUT -p tcp --dport 5254 -j MARK --set-mark 1
        sudo tc qdisc add dev ens5 root handle 1: prio
        sudo tc qdisc add dev ens5 parent 1:1 handle 10: netem delay {options.latencyMs}ms
        sudo tc filter add dev ens5 protocol ip parent 1:0 prio 1 handle 1 fw flowid 1:1
        """,
        )

    title("CREATE DATA")
    # The operation type is "write" to create some data. The real benchmark's operation is "read".
    run_command(
        f"./build/Examples/Benchmark --cluster={','.join(SERVERS)} "
        f"--size={options.size} --threads={options.threads} --operation-type=write "
        f"--timeout=30s --operations={options.threads}"
    )

    title("BENCHMARK")
    run_command(
        f"./build/Examples/Benchmark --cluster={','.join(SERVERS)} "
        f"--size={options.size} --threads={options.threads} --operation-type=read "
        f"--timeout=30s --operations={options.operations} --resultsFile=one_result.txt"
    )

    title("CLEANUP")
    for addr in SERVERS:
        run_ssh_command(
            addr,
            f"""
        sudo killall -q -9 perf LogCabin Reconfigure || true
        sudo tc qdisc del dev ens5 root || true
        sudo iptables -t mangle -F || true""",
        )

    reader = csv.DictReader(open("one_result.txt"))
    row = next(reader)
    stats.append(
        options=options,
        result=BenchmarkResult(
            opsPerSec=float(row["opsPerSec"]),
            p50latencyNanos=float(row["p50latencyNanos"]),
            p90latencyNanos=float(row["p90latencyNanos"]),
            p95latencyNanos=float(row["p95latencyNanos"]),
        )
    )

    stats.save()


if __name__ == "__main__":
    stats = Stats()
    stats.load()

    for quorumCheckOnRead, leaseEnabled, deferCommitEnabled, inheritLeaseEnabled in [
        (False, False, False, False),
        (True, False, False, False),
        (False, True, True, True),
    ]:
        for latencyMs in range(11):
            options = BenchmarkOptions(
                latencyMs=latencyMs,
                quorumCheckOnRead=quorumCheckOnRead,
                leaseEnabled=leaseEnabled,
                deferCommitEnabled=deferCommitEnabled,
                inheritLeaseEnabled=inheritLeaseEnabled,
            )

            n_already = len([r for r in stats.rows if r.options == options])
            n_needed = max(0, args.trials - n_already)
            print(f"{n_needed} trials for {options}")
            for _ in range(n_needed):
                run_benchmark(options, stats)

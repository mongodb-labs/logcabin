"""Test the effect of throughput on latency, with or without leases."""

import argparse
import csv
import time
import traceback
from dataclasses import dataclass

from lib import (
    BenchmarkOptions,
    BenchmarkResult,
    run_command,
    run_ssh_command,
    title,
    write_config_files,
    Stats,
)


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--servers", type=str, required=True, help="Comma-separated list of addresses"
)
parser.add_argument(
    "--trials", type=int, default=1, help="Number of trials for each config"
)
args = parser.parse_args()
SERVERS = args.servers.split(",")

EXPERIMENT_DURATION_SEC = 15

@dataclass(kw_only=True)
class LatencyVsThroughputBenchmarkOptions(BenchmarkOptions):
    reads: int
    writes: int


def run_benchmark(options: LatencyVsThroughputBenchmarkOptions, stats: Stats):
    write_config_files(SERVERS, options)

    for server_id, addr in enumerate(SERVERS, start=1):
        title(f"SETUP {addr}")
        run_ssh_command(
            addr,
            f"""
        # cleanup past rules from network_latency_experiment.py
        sudo tc qdisc del dev ens5 root > /dev/null 2>&1 || true
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
        f"./build/Examples/Reconfigure --cluster={SERVERS[0]} set {' '.join(SERVERS)}",
        timeout=30,
    )

    title("HELLOWORLD")
    run_command(
        f"./build/Examples/HelloWorld --cluster={','.join(SERVERS)}", timeout=30
    )

    title("EXPERIMENT")
    try:
        run_command(
            f"./build/Examples/NetworkLatencyTest --cluster={','.join(SERVERS)} "
            f"--size={options.size} "
            f"--timeout={EXPERIMENT_DURATION_SEC}s "
            f"--reads={options.reads} "
            f"--writes={options.writes} "
            f"--resultsFile=one_result.txt",
            timeout=90,
        )
    finally:
        title("CLEANUP")
        for addr in SERVERS:
            run_ssh_command(
                addr,
                f"""
            sudo killall -q -9 perf LogCabin Reconfigure || true
            sudo tc qdisc del dev ens5 root || true
            sudo iptables -t mangle -F || true""",
            )
            
    for row in csv.DictReader(open("one_result.txt")):
        stats.append(
            options=options,
            result=BenchmarkResult(
                operationType=row["operationType"],
                opsPerSec=float(row["opsPerSec"]),
                p50latencyNanos=float(row["p50latencyNanos"]),
                p90latencyNanos=float(row["p90latencyNanos"]),
                p95latencyNanos=float(row["p95latencyNanos"]),
            ),
        )

    stats.save()
    time.sleep(30)  # Might help with port reuse issues?


if __name__ == "__main__":
    stats = Stats(options_type=LatencyVsThroughputBenchmarkOptions,
                  result_type=BenchmarkResult,
                  csv_path=__file__.replace(".py", ".csv"))
    stats.load()

    for (quorum, ongaro, leaseGuard, deferCommit, inheritLease) in [
        (False, False, False, False, False), # inconsistent
        (True, False, False, False, False), # quorum check
        (False, True, False, False, False), # ongaro lease
        (False, False, True, True, True), # leaseguard with optimizations
    ]:
        for kilo_ops_per_sec in range(5, 85, 5):
            for write_ratio in (0, 0.25, 0.5, 0.75):
                operations = EXPERIMENT_DURATION_SEC * kilo_ops_per_sec * 1000
                writes = int(operations * write_ratio)
                reads = operations - writes
                options = LatencyVsThroughputBenchmarkOptions(
                    reads=reads,
                    writes=writes,
                    quorumCheckOnRead=quorum,
                    ongaroLeaseEnabled=ongaro,
                    leaseGuardEnabled=leaseGuard,
                    deferCommitEnabled=deferCommit,
                    inheritLeaseEnabled=inheritLease,
                )

                # Writes and reads are separate rows, so there are 2 rows per trial.
                n_already = len([r for r in stats.rows if r.options == options]) // 2
                n_needed = max(0, args.trials - n_already)
                print(f"{n_needed} trials for {options}")
                for _ in range(n_needed):
                    while True:
                        try:
                            run_benchmark(options, stats)
                            break
                        except Exception as e:
                            traceback.print_exc()
                            print("RETRYING")
                            
    print("ALL DONE")

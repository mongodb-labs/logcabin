"""Test the effect of network latency, with or without leases."""

import argparse
import csv
import os
import time
import subprocess
from dataclasses import dataclass, asdict, fields
from datetime import datetime
from typing import Any

import paramiko


parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--servers", type=str, required=True, help="Comma-separated list of addresses"
)
parser.add_argument(
    "--trials", type=int, default=5, help="Number of trials for each config"
)
args = parser.parse_args()
SERVERS = args.servers.split(",")


def deserialize_field(field_type: Any, value: str) -> Any:
    """Convert a string value to the appropriate type."""
    if field_type == int:
        return int(value)
    elif field_type == float:
        return float(value)
    elif field_type == str:
        return value
    elif field_type == list[str]:
        return eval(value)
    elif field_type == bool:
        return value == "True"
    elif field_type == datetime:
        return datetime.fromisoformat(value)
    else:
        raise ValueError(f"Unsupported type: {field_type}")


@dataclass
class BenchmarkOptions:
    # camelCase for consistency with the names in LogCabin config file and C++.
    latencyMs: int = 0
    operationType: str = ""
    quorumCheckOnRead: bool = False
    leaseEnabled: bool = False
    deferCommitEnabled: bool = False
    inheritLeaseEnabled: bool = False
    size: int = 1024
    threads: int = 100
    operations: int = 100000

    def __post_init__(self):
        if self.operationType not in {"read", "write"}:
            raise ValueError(
                f"operationType should be 'read' or 'write', not '{self.operationType}'"
            )
        if self.deferCommitEnabled and not self.leaseEnabled:
            raise ValueError("deferCommitEnabled requires leaseEnabled")
        if self.inheritLeaseEnabled and not self.leaseEnabled:
            raise ValueError("inheritLeaseEnabled requires leaseEnabled")


class Stats:
    CSV_FILE_PATH = os.path.basename(__file__) + "/results.csv"

    @dataclass
    class Row(BenchmarkOptions):
        # Must have default vals since they follow BenchmarkOptions' fields.
        recordedAt: datetime = None
        opsPerSec: float = None
        p50latencyMicros: float = None
        p90latencyMicros: float = None
        p95latencyMicros: float = None

        @staticmethod
        def from_benchmark_options(
            options: BenchmarkOptions,
            opsPerSec: float,
            p50latencyMicros: float,
            p90latencyMicros: float,
            p95latencyMicros: float,
        ) -> "Stats.Row":
            data = asdict(options)

            return Stats.Row(
                **data,
                recordedAt=datetime.now().isoformat(),
                opsPerSec=opsPerSec,
                p50latencyMicros=p50latencyMicros,
                p90latencyMicros=p90latencyMicros,
                p95latencyMicros=p95latencyMicros,
            )

        def matches(self, options: BenchmarkOptions):
            for f in fields(options):
                if getattr(self, f.name) != getattr(options, f.name):
                    return False

            return True

    def __init__(self):
        self.rows: list[Stats.Row] = []

    def load(self):
        if os.path.exists(Stats.CSV_FILE_PATH):
            with open(Stats.CSV_FILE_PATH, mode="r") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    raise ValueError("CSV file has no columns.")

                for f in fields(Stats.Row):
                    if f.name not in reader.fieldnames:
                        raise ValueError(f"Missing required column: {f}")

                for row in reader:
                    self.rows.append(
                        Stats.Row(
                            **{
                                f.name: deserialize_field(f.type, row[f.name])
                                for f in fields(Stats.Row)
                            }
                        )
                    )

    def append(self, **kwargs):
        self.rows.append(Stats.Row.from_benchmark_options(**kwargs))

    def save(self):
        with open(Stats.CSV_FILE_PATH, mode="w") as f:
            writer = csv.DictWriter(f, fieldnames=(f.name for f in fields(Stats.Row)))
            writer.writeheader()
            writer.writerows(asdict(row) for row in self.rows)


def title(t):
    print(f"\n==== {t} {('======'*10)[:75 - len(t)]}\n")


def run_command(command):
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    try:
        for line in process.stdout:
            print(line, end="")
    finally:
        process.wait()

    if process.returncode:
        raise subprocess.CalledProcessError(process.returncode, command)


def run_ssh_command(host, command):
    client = paramiko.SSHClient()
    # Automatically add the server's host key if it's not already known
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host,
            username="ubuntu",
            key_filename="/home/ubuntu/.ssh/jesse-2024.pem",
        )
        # set -e to stop on error
        stdin, stdout, stderr = client.exec_command(f"set -e\n{command}")
        for s in stdout, stderr:
            out = s.read().decode().strip()
            if out:
                print(out)
        exit_code = stdout.channel.recv_exit_status()
        if exit_code:
            raise subprocess.CalledProcessError(exit_code, f"{host} {command}")
    finally:
        client.close()


def run_benchmark(options: BenchmarkOptions, stats: Stats):
    def bul(b: bool):
        return "true" if b else "false"

    for server_id, addr in enumerate(SERVERS, start=1):
        with open(f"conf{server_id}.conf", "w") as f:
            # Write the conf file locally, sshfs will copy it to all servers.
            f.write(
                f"""\
    serverId = {server_id}
    listenAddresses = {addr}
    clusterUUID = foo
    storagePath = /tmp/logcabin
    logPolicy = NOTICE
    snapshotMinLogSize = 99999999999
    tcpConnectTimeoutMilliseconds = {max(10000, 10 * options.latencyMs)}
    electionTimeoutMilliseconds = {max(10000, 10 * options.latencyMs)}
    delta = {max(1000, 5 * options.latencyMs)}
    quorumCheckOnRead = {bul(options.quorumCheckOnRead)}
    leaseEnabled = {bul(options.leaseEnabled)}
    deferCommitEnabled = {bul(options.deferCommitEnabled)}
    inheritLeaseEnabled = {bul(options.inheritLeaseEnabled)}
    """
            )

    time.sleep(5)

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

    title("BENCHMARK")
    run_command(
        f"./build/Examples/Benchmark --cluster={','.join(SERVERS)} "
        f"--size={options.size} --threads={options.threads} --operation-type={options.operationType} "
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
        opsPerSec=float(row["opsPerSec"]),
        p50latencyMicros=float(row["p50latencyMicros"]),
        p90latencyMicros=float(row["p90latencyMicros"]),
        p95latencyMicros=float(row["p95latencyMicros"]),
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
        for latencyMs in range(0, 501, 100):
            for operationType in ("read", "write"):
                options = BenchmarkOptions(
                    latencyMs=latencyMs,
                    operationType=operationType,
                    quorumCheckOnRead=quorumCheckOnRead,
                    leaseEnabled=leaseEnabled,
                    deferCommitEnabled=deferCommitEnabled,
                    inheritLeaseEnabled=inheritLeaseEnabled,
                )

                n_already = len([r for r in stats.rows if r.matches(options)])
                n_needed = max(0, args.trials - n_already)
                print(f"{n_needed} trials for {options}")
                for _ in range(n_needed):
                    run_benchmark(options, stats)

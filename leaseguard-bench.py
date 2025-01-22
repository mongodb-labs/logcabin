import argparse
import csv
import os
import time
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime

import paramiko  # pip install paramiko


parser = argparse.ArgumentParser(
    description="Run a script with specified latency and servers."
)
parser.add_argument(
    "--servers", type=str, required=True, help="Comma-separated list of addresses"
)
args = parser.parse_args()


@dataclass
class BenchmarkOptions:
    # camelCase for consistency with the names in LogCabin config file and C++.
    servers: list[str]
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


def write_result(options: BenchmarkOptions, ops_per_sec: float):
    csv_file_path = "results.csv"
    row = asdict(options)
    row["date"] = datetime.now().isoformat()
    row["operationsPerSec"] = ops_per_sec
    del row["servers"]

    if os.path.exists(csv_file_path):
        with open(csv_file_path, mode="r", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("CSV file has no columns.")
            # Ensure all required columns are present
            for col in row.keys():
                if col not in reader.fieldnames:
                    raise ValueError(f"Missing required column: {col}")
    else:
        with open(csv_file_path, mode="w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=row.keys())
            writer.writeheader()

    with open(csv_file_path, mode="a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        writer.writerow(row)

    print(row)


def run_benchmark(options: BenchmarkOptions):
    def bul(b: bool):
        return "true" if b else "false"
        
    
    for server_id, addr in enumerate(options.servers, start=1):
        with open(f"conf{server_id}.conf", "w") as f:
            # Write the conf file locally, sshfs will copy it to all servers.
            f.write(
                f"""\
    serverId = {server_id}
    listenAddresses = {addr}
    clusterUUID = foo
    storagePath = storage{server_id}
    logPolicy = NOTICE
    snapshotMinLogSize = 99999999999
    tcpConnectTimeoutMilliseconds = {max(10000, 10 * options.latencyMs)}
    electionTimeoutMilliseconds = {max(10000, 10 * options.latencyMs)}
    quorumCheckOnRead = {bul(options.quorumCheckOnRead)}
    leaseEnabled = {bul(options.leaseEnabled)}
    deferCommitEnabled = {bul(options.deferCommitEnabled)}
    inheritLeaseEnabled = {bul(options.inheritLeaseEnabled)}
    """
            )

    time.sleep(5)

    for server_id, addr in enumerate(options.servers, start=1):
        title(f"SETUP {addr}")
        run_ssh_command(
            addr,
            f"""
        sudo tc qdisc del dev ens5 root > /dev/null 2>&1 || true # cleanup past rules
        cd logcabin
        killall -q -9 perf LogCabin Reconfigure || true
        rm -rf storage{server_id} {server_id}.log
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
        f"./build/Examples/Reconfigure --cluster={options.servers[0]} set {' '.join(options.servers)}"
    )

    title("HELLOWORLD")
    run_command(f"./build/Examples/HelloWorld --cluster={','.join(options.servers)}")

    for server_id, addr in enumerate(options.servers, start=1):
        title(f"CONFIG NETWORK {addr}")
        run_ssh_command(
            addr,
            f"""
        sudo iptables -t mangle -A OUTPUT -p tcp --dport 5254 -j MARK --set-mark 1
        sudo tc qdisc add dev ens5 root handle 1: prio
        sudo tc qdisc add dev ens5 parent 1:1 handle 10: netem delay {options.latencyMs}ms
        sudo tc filter add dev ens5 protocol ip parent 1:0 prio 1 handle 1 fw flowid 1:1
        """)

    title("BENCHMARK")
    run_command(
        f"./build/Examples/Benchmark --cluster={','.join(options.servers)} "
        f"--size={options.size} --threads={options.threads} --operation-type={options.operationType} "
        f"--timeout=30s --operations={options.operations} --opsPerSecFile=opspersec.txt"
    )

    title("CLEANUP")
    for addr in options.servers:
        run_ssh_command(
            addr,
            f"""
        sudo killall -q -9 perf LogCabin Reconfigure || true
        sudo tc qdisc del dev ens5 root || true
        sudo iptables -t mangle -F || true""",
        )

    with open("opspersec.txt") as f:
        write_result(options=options, ops_per_sec=float(f.read().strip()))


if __name__ == "__main__":
    for quorumCheckOnRead, leaseEnabled, deferCommitEnabled, inheritLeaseEnabled in [
        (False, False, False, False),
        (True, False, False, False),
        (False, True, False, False),
        (False, True, True, False),
        (False, True, False, True),
        (False, True, True, True),
    ]:
        for latencyMs in range(0, 501, 100):
            for operationType in ("read", "write"):
                run_benchmark(
                BenchmarkOptions(
                    servers=args.servers.split(","),
                    latencyMs=latencyMs,
                    operationType=operationType,
                    quorumCheckOnRead=quorumCheckOnRead,
                    leaseEnabled=leaseEnabled,
                    deferCommitEnabled=deferCommitEnabled,
                    inheritLeaseEnabled=inheritLeaseEnabled,
                )
            )

import io
import subprocess
import threading
import time
from typing import Any
from dataclasses import dataclass, fields, asdict
from datetime import datetime
import csv
import os
from typing import Type

import paramiko


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


def dataclass_from_row(dataclass_type, row):
    field_types = {f.name: f.type for f in fields(dataclass_type)}
    kwargs = {
        name: deserialize_field(field_types[name], row[name]) for name in field_types
    }
    return dataclass_type(**kwargs)


def dataclass_fieldnames(dataclass_type):
    return [f.name for f in fields(dataclass_type)]


class Stats:
    """Generic CSV-backed stats manager for an (options_dataclass, result_dataclass).

    Usage:
      stats = Stats(options_type=LatencyBenchmarkOptions, result_type=BenchmarkResult,
                    csv_path="/path/to/file.csv")
      stats.load()
      stats.append(options, result)
      stats.save()
    """

    def __init__(
        self,
        options_type: Type,
        result_type: Type,
        csv_path: str,
    ) -> None:
        self.options_type = options_type
        self.result_type = result_type
        self.csv_path = csv_path
        self.rows: list[dict] = []

    class Row:
        def __init__(self, options, result):
            self.options = options
            self.result = result

    def load(self):
        try:
            if os.path.exists(self.csv_path):
                with open(self.csv_path, mode="r") as f:
                    reader = csv.DictReader(f)
                    if not reader.fieldnames:
                        raise ValueError(f"{self.csv_path} has no columns.")

                    for row in reader:
                        options = dataclass_from_row(self.options_type, row)
                        result = dataclass_from_row(self.result_type, row)
                        self.rows.append(Stats.Row(options=options, result=result))
        except Exception as e:
            print(f"Failed to load existing stats from {self.csv_path}: {e}")
            raise

    def append(self, options, result):
        self.rows.append(Stats.Row(options=options, result=result))

    def save(self):
        with open(self.csv_path, mode="w") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=(
                    dataclass_fieldnames(self.options_type)
                    + dataclass_fieldnames(self.result_type)
                ),
            )
            writer.writeheader()
            writer.writerows(asdict(row.options) | asdict(row.result) for row in self.rows)


@dataclass(kw_only=True)
class BenchmarkOptions:
    # camelCase for consistency with the names in LogCabin config file and C++.
    quorumCheckOnRead: bool = False
    ongaroLeaseEnabled: bool = False
    leaseGuardEnabled: bool = False
    deferCommitEnabled: bool = False
    inheritLeaseEnabled: bool = False
    size: int = 1024
    electionTimeoutMilliseconds: int = 500
    delta: int = 500  # Milliseconds.

    def __post_init__(self):
        if sum([self.quorumCheckOnRead, self.ongaroLeaseEnabled, self.leaseGuardEnabled]) > 1:
            raise ValueError(
                "quorumCheckOnRead, ongaroLeaseEnabled, leaseGuardEnabled are mutually exclusive")
        if self.deferCommitEnabled and not self.leaseGuardEnabled:
            raise ValueError("deferCommitEnabled requires leaseGuardEnabled")
        if self.inheritLeaseEnabled and not self.leaseGuardEnabled:
            raise ValueError("inheritLeaseEnabled requires leaseGuardEnabled")


@dataclass
class BenchmarkResult:
    operationType: str
    opsPerSec: float
    p50latencyNanos: float
    p90latencyNanos: float
    p95latencyNanos: float


def run_command(command: str, quiet: bool = False, timeout: float = None) -> str:
    print(command)
    process = None
    output = io.StringIO()

    def target():
        nonlocal process

        process = subprocess.Popen(
            command.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
        )

        try:
            for line in process.stdout:
                if not quiet:
                    print(line, end="")
                output.write(line)
        finally:
            process.wait()

    thread = threading.Thread(target=target)
    thread.start()
    thread.join(timeout)

    if thread.is_alive():
        subprocess.call(["kill", str(process.pid)])
        thread.join()
        raise Exception(f"Command '{command}' timed out after {timeout} seconds")

    if process.returncode != 0:
        raise subprocess.CalledProcessError(process.returncode, command)

    return output.getvalue()


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
        _, stdout, stderr = client.exec_command(f"set -e\n{command}")
        for s in stdout, stderr:
            out = s.read().decode().strip()
            if out:
                print(out)
        exit_code = stdout.channel.recv_exit_status()
        if exit_code:
            raise subprocess.CalledProcessError(exit_code, f"{host} {command}")
    finally:
        client.close()


def title(t):
    print(f"\n{t} {('======'*10)[:75 - len(t)]}\n")


def write_config_files(
    servers: list[str],
    options: BenchmarkOptions,
    non_candidate_ids: list[int] | None = None,  # TODO: remove
):
    def bul(b: bool):
        return "true" if b else "false"

    for server_id, addr in enumerate(servers, start=1):
        electable = not non_candidate_ids or server_id not in non_candidate_ids
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
tcpConnectTimeoutMilliseconds = 10000
electionTimeoutMilliseconds = {options.electionTimeoutMilliseconds}
electable = {bul(electable)}
delta = {options.delta}
quorumCheckOnRead = {bul(options.quorumCheckOnRead)}
leaseGuardEnabled = {bul(options.leaseGuardEnabled)}
ongaroLeaseEnabled = {bul(options.ongaroLeaseEnabled)}
deferCommitEnabled = {bul(options.deferCommitEnabled)}
inheritLeaseEnabled = {bul(options.inheritLeaseEnabled)}
electionTimeoutRandomizationDisabled = true
"""
            )

    time.sleep(5)  # Let sshfs copy the files.

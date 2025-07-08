import io
import subprocess
import threading
import time
from typing import Any
from dataclasses import dataclass, fields
from datetime import datetime

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


@dataclass(kw_only=True)
class BenchmarkOptions:
    # camelCase for consistency with the names in LogCabin config file and C++.
    quorumCheckOnRead: bool = False
    leaseEnabled: bool = False
    deferCommitEnabled: bool = False
    inheritLeaseEnabled: bool = False
    size: int = 1024
    electionTimeoutMilliseconds: int = 500
    delta: int = 500  # Milliseconds.

    def __post_init__(self):
        if self.deferCommitEnabled and not self.leaseEnabled:
            raise ValueError("deferCommitEnabled requires leaseEnabled")
        if self.inheritLeaseEnabled and not self.leaseEnabled:
            raise ValueError("inheritLeaseEnabled requires leaseEnabled")


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
            key_filename="/home/ubuntu/.ssh/XXX-2024.pem",
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
leaseEnabled = {bul(options.leaseEnabled)}
deferCommitEnabled = {bul(options.deferCommitEnabled)}
inheritLeaseEnabled = {bul(options.inheritLeaseEnabled)}
electionTimeoutRandomizationDisabled = true
"""
            )

    time.sleep(5)  # Let sshfs copy the files.

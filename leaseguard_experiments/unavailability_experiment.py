"""Test the effect of leases and optimizations on read/write availability."""

import argparse
from dataclasses import dataclass
import os.path
import subprocess
import threading
import time
from datetime import datetime

import pandas as pd
import paramiko

from lib import (
    BenchmarkOptions,
    run_command,
    run_ssh_command,
    title,
    write_config_files,
)


ELECTION_TIMEOUT_MS = 500
# Long lease to explore inherited read lease optimization.
LEASE_TIMEOUT_MS = 2 * ELECTION_TIMEOUT_MS
# Trigger regicide part way into the experiment.
KILL_LEADER_TIME_MS = ELECTION_TIMEOUT_MS


@dataclass(kw_only=True)
class UnavailabilityBenchmarkOptions(BenchmarkOptions):
    writes_per_ms: int
    reads_per_ms: int
    

def _make_options():
    options = {}
    for (
        quorumCheckOnRead,
        leaseGuardEnabled,
        deferCommitEnabled,
        inheritLeaseEnabled,
        writes_per_ms,
        reads_per_ms,
        name,
    ) in [
        (False, False, False, False, 10, 20, "inconsistent"),
        (True, False, False, False, 1, 2, "quorum"),  # Can't keep up with other configs
        (False, True, False, False, 10, 20, "lease"),
        (False, True, True, False, 10, 20, "defer\ncommit"),
        (False, True, True, True, 10, 20, "inherit\nlease"),
    ]:
        options[name] = UnavailabilityBenchmarkOptions(
            quorumCheckOnRead=quorumCheckOnRead,
            leaseGuardEnabled=leaseGuardEnabled,
            deferCommitEnabled=deferCommitEnabled,
            inheritLeaseEnabled=inheritLeaseEnabled,
            electionTimeoutMilliseconds=ELECTION_TIMEOUT_MS,
            delta=2 * ELECTION_TIMEOUT_MS,  # Test lease expiration > election timeout.
            writes_per_ms=writes_per_ms,
            reads_per_ms=reads_per_ms,
        )
    return options


OPTIONS = _make_options()


def time_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def is_leader(server: str) -> bool:
    try:
        out = run_command(
            f"./build/Client/ServerControl --server={server} --timeout=1s stats get",
            timeout=5,
            quiet=True,
        )
        return "state: LEADER" in out
    except Exception as e:
        print(f"Failed to check leader for {server}: {e}")
        return False


def kill_leader(when: float, servers: list[str]):
    for s in servers:
        if is_leader(s):
            # To kill the leader at the precisely right time, open the channel then wait.
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=s,
                username="ubuntu",
                key_filename="/home/ubuntu/.ssh/jesse-2024.pem",
            )
            session = client.get_transport().open_session()
            sleep_duration = when - time.time()
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            print(f"{time_str()} Kill leader {s}")
            session.exec_command("killall -9 LogCabin")
            client.close()
            print(f"{time_str()} Killed leader {s}")
            break
    else:
        raise RuntimeError("No leader found to kill")


def main(servers: list[str], enabled_configs: list[BenchmarkOptions]):
    for option_index, options in enumerate(OPTIONS.values()):
        if options not in enabled_configs:
            continue

        print(options)
        # Since electionTimeoutRandomizationDisabled is true, when we kill serverId 1, serverId 2
        # and 3 can compete for election. Fix that by disabling serverId 2. Now we have a new
        # problem: if serverId 2 has a longer log than 3 at the time we kill 1, then 3 can't win the
        # election. In that case just retry. This is all a hack to make the election precisely
        # electionTimeoutMilliseconds long for the sake of a pretty chart.
        write_config_files(servers, options, non_candidate_ids=[2])
        succeeded = False
        for retry in range(100):
            for server_id, addr in enumerate(servers, start=1):
                title(f"SETUP {addr}")
                run_ssh_command(
                    addr,
                    f"""
cd logcabin
killall -9 perf LogCabin Reconfigure || true
sleep 1
rm -rf /tmp/logcabin {server_id}.log
""",
                )

                if server_id == 1:
                    run_ssh_command(
                        addr,
                        """
cd logcabin
ulimit -c unlimited
./build/LogCabin --config conf1.conf --bootstrap
""",
                    )

                run_ssh_command(
                    addr,
                    f"""
cd logcabin
ulimit -c unlimited
nohup ./build/LogCabin --config conf{server_id}.conf --log {server_id}.log >{server_id}.out 2>&1 </dev/null &
ps aux | grep LogCabin""",
                )

            time.sleep(5)

            title("RECONFIGURE")
            try:
                run_command(
                    f"./build/Examples/Reconfigure --cluster={servers[0]} set {' '.join(servers)}",
                    timeout=30,  # This command is prone to hanging if a server crashes
                )
            except Exception as e:
                print(f"RETRY: attempt {retry}, Reconfigure failed: {e}")
                continue

            title("HELLOWORLD")
            run_command(f"./build/Examples/HelloWorld --cluster={','.join(servers)}", timeout=30)

            title("EXPERIMENT")
            t = threading.Thread(
                target=kill_leader,
                kwargs={
                    "when": time.time() + KILL_LEADER_TIME_MS / 1000,
                    "servers": servers,
                },
            )
            t.start()
            print(f"{time_str()} Start UnavailabilityTest")
            try:
                unavailability_test_cmd = (
                    f"./build/Examples/UnavailabilityTest --cluster={','.join(servers)} "
                    f"--size={options.size} --timeout={3 * LEASE_TIMEOUT_MS}ms "
                    f"--writesPerMs={options.writes_per_ms} --readsPerMs={options.reads_per_ms} "
                    f"--out=unavailability_result.txt")
                if args.valgrind:
                    unavailability_test_cmd = f"valgrind {unavailability_test_cmd}"
                run_command(unavailability_test_cmd, timeout=30)
            except Exception as e:
                # Probably serverId 3 didn't become leader. Retry.
                print(f"RETRY: UnavailabilityTest failed: {e}")
                continue

            t.join()
            time.sleep(5)
            if is_leader(servers[2]):
                print(f"{time_str()} SUCCESS: serverId 3 became leader")
            else:
                print(
                    f"{time_str()} RETRY: attempt {retry} failed, serverId 3 didn't become leader"
                )
                continue

            title("CLEANUP")
            for addr in servers:
                run_ssh_command(
                    addr, "sudo killall -q -9 perf LogCabin Reconfigure || true"
                )

            succeeded = True
            break

        if not succeeded:
            raise RuntimeError(f"Failed to complete experiment after {retry} tries")

        df = pd.read_csv("unavailability_result.txt")
        df["quorumCheckOnRead"] = options.quorumCheckOnRead
        df["leaseGuardEnabled"] = options.leaseGuardEnabled
        df["deferCommitEnabled"] = options.deferCommitEnabled
        df["inheritLeaseEnabled"] = options.inheritLeaseEnabled
        csv_path = f"{os.path.splitext(__file__)[0]}-{option_index}.csv"
        df.to_csv(csv_path, index=False)
        print(csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--servers", type=str, required=True, help="Comma-separated list of addresses"
    )
    option_names = [name.replace("\n", "") for name in OPTIONS.keys()]
    parser.add_argument(
        "--config",
        action="append",
        choices=option_names,
        help="Which benchmark configs to enable",
    )
    parser.add_argument(
        "--valgrind",
        action="store_true",
        help="Run UnavailabilityTest under Valgrind",
    )
    args = parser.parse_args()
    SERVERS = args.servers.split(",")
    if args.config:
        ENABLED_CONFIGS = [
            options
            for name, options in OPTIONS.items()
            if name.replace("\n", "") in args.config
        ]
    else:
        ENABLED_CONFIGS = list(OPTIONS.values())
    main(SERVERS, ENABLED_CONFIGS)

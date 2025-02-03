"""Test the effect of leases and optimizations on read/write availability."""

import argparse
import os.path
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
CSV_PATH = (
    f"leaseguard_experiments/{os.path.splitext(os.path.basename(__file__))[0]}.csv"
)


OPTIONS = {}


def _make_options():  # lease without deferCommit hangs
    for (
        quorumCheckOnRead,
        leaseEnabled,
        deferCommitEnabled,
        inheritLeaseEnabled,
        name,
    ) in [
        (False, False, False, False, "inconsistent"),
        (True, False, False, False, "quorum"),
        (False, True, False, False, "lease"),
        (False, True, True, False, "defer\ncommit"),
        (False, True, True, True, "inherit\nlease"),
    ]:
        OPTIONS[name] = BenchmarkOptions(
            quorumCheckOnRead=quorumCheckOnRead,
            leaseEnabled=leaseEnabled,
            deferCommitEnabled=deferCommitEnabled,
            inheritLeaseEnabled=inheritLeaseEnabled,
            operations=9999999,  # Let Benchmark.cc's timeout end the trial.
            threads=10,
        )


_make_options()


def kill_leader(when: float):
    for s in SERVERS:
        out = run_command(
            f"./build/Client/ServerControl --server={s} stats get", quiet=True
        )
        if "state: LEADER" in out:
            # To kill the leader at the precisely right time, open the channel then wait.
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(
                hostname=s,
                username="ubuntu",
                key_filename="/home/ubuntu/.ssh/jesse-2024.pem",
            )
            session = client.get_transport().open_session()
            time.sleep(when - time.time())
            print(f"Kill leader {s} at {datetime.now().strftime("%S.%f")}")
            session.exec_command("killall -9 LogCabin")
            client.close()
            print(f"Killed leader {s} at {datetime.now().strftime("%S.%f")}")
            break


def main():
    if os.path.exists(CSV_PATH):
        os.remove(CSV_PATH)

    combined_df: pd.DataFrame | None = None
    for options in OPTIONS.values():
        print(options)
        write_config_files(SERVERS, options)

        for server_id, addr in enumerate(SERVERS, start=1):
            title(f"SETUP {addr}")
            run_ssh_command(
                addr,
                f"""
cd logcabin
killall -9 perf LogCabin Reconfigure || true
rm -rf /tmp/logcabin {server_id}.log
""",
            )

            if server_id == 1:
                run_ssh_command(
                    addr,
                    "cd logcabin; ./build/LogCabin --config conf1.conf --bootstrap",
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

        title("EXPERIMENT")
        t = threading.Thread(
            target=kill_leader,
            kwargs={"when": time.time() + KILL_LEADER_TIME_MS / 1000},
        )
        t.start()
        current_time = datetime.now().strftime("%S.%f")
        print(f"Start UnavailabilityTest at {current_time}")
        run_command(
            f"./build/Examples/UnavailabilityTest --cluster={','.join(SERVERS)} "
            f"--size={options.size} --timeout={3 * LEASE_TIMEOUT_MS}ms "
            f"--out=unavailability_result.txt"
        )

        t.join()
        title("CLEANUP")
        for addr in SERVERS:
            run_ssh_command(
                addr, "sudo killall -q -9 perf LogCabin Reconfigure || true"
            )

        df = pd.read_csv("unavailability_result.txt")
        df["quorumCheckOnRead"] = options.quorumCheckOnRead
        df["leaseEnabled"] = options.leaseEnabled
        df["deferCommitEnabled"] = options.deferCommitEnabled
        df["inheritLeaseEnabled"] = options.inheritLeaseEnabled
        if combined_df is None:
            combined_df = df
        else:
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # Write it out each trial for debugging.
        combined_df.to_csv(CSV_PATH, index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--servers", type=str, required=True, help="Comma-separated list of addresses"
    )
    args = parser.parse_args()
    SERVERS = args.servers.split(",")
    main()

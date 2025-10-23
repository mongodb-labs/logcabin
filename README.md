LeaseGuard
==========

This is the LeaseGuard branch of the LogCabin repo. LeaseGuard is a new leader lease protocol; the
changes on this branch include a prototype implementation of LeaseGuard on top of LogCabin, plus an
implementation of Ongaro's lease protocol for comparison (Ongaro's thesis §6.4.1).

## Instance setup

First, set up 4 AWS EC2 instances (1 client, 3 servers). Ubuntu Server 24.04, 64-bit ARM, m7g.xlarge, all in one placement group. Install dependencies:

```
sudo apt install ubuntu-dbgsym-keyring
echo "deb http://ddebs.ubuntu.com $(lsb_release -cs) main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-updates main restricted universe multiverse
deb http://ddebs.ubuntu.com $(lsb_release -cs)-proposed main restricted universe multiverse" | sudo tee -a /etc/apt/sources.list.d/ddebs.list
sudo apt-get update
sudo apt-get install -y libcrypto++-dev libcrypto++-doc libcrypto++-utils scons protobuf-compiler g++ libtbb-dev linux-headers-$(uname -r) make gcc gdb emacs zsh libstdc++6-dbgsym libc6-dbgsym linux-image-$(uname -r)-dbgsym python3-venv
touch ~/.hushlogin
sudo reboot 
```

Enable ssh passwordless auth among the four nodes by generating a key pair and installing it in `~/.ssh` on all of them and configuring `~/.ssh/config` and `~/.ssh/authorized_keys` (exercise for the reader).

## Tightly synchronized clocks

Do the following on all four nodes. [Instructions inspired by Yugabyte](https://www.yugabyte.com/blog/aws-clock-synchronization/).

Install ENA on servers:

```
git clone https://github.com/amzn/amzn-drivers.git
cd amzn-drivers/kernel/linux/ena
ENA_PHC_INCLUDE=1 make
sudo cp ena.ko /lib/modules/`uname -r`
sudo modprobe ptp # must do before insmod ena.ko?
sudo rmmod ena; sudo insmod /lib/modules/`uname -r`/ena.ko phc_enable=1
ls /dev/ptp0  # should exist now
echo 'refclock PHC /dev/ptp0 poll 0 delay 0.000010 prefer' | sudo tee /etc/chrony/chrony.conf
sudo systemctl daemon-reload
sudo systemctl restart chronyd
```

After each reboot for some reason I have to do the following:

```
ls /lib/modules/`uname -r`/ena.ko # make sure it's there
sudo rmmod ena && sudo insmod /lib/modules/`uname -r`/ena.ko phc_enable=1
sudo systemctl daemon-reload
sudo systemctl restart chronyd
# should have clock status SYNCHRONIZED
~/clock-bound/examples/c/src/clockbound_now
```

## clock-bound

Install Amazon's clock-bound daemon and library on all nodes.

I'm running on AWS, m6g.2xlarge in us-east, so I have AWS TimeSync and chronyd by default. I follow [the instructions](https://github.com/aws/clock-bound/blob/main/clock-bound-d/README.md) and increase maxclockerror in /etc/chrony/chrony.conf.

````
curl https://sh.rustup.rs -sSf | sh  # Install Cargo
git clone git@github.com:aws/clock-bound.git; cd clock-bound; ~/.cargo/bin/cargo build --release
sudo usermod -aG _chrony ubuntu  # let ubuntu user access chronyd's domain socket
sudo mv target/release/clockbound /usr/local/bin/clockbound
sudo chown _chrony:_chrony /usr/local/bin/clockbound

sudo tee /usr/lib/systemd/system/clockbound.service <<EOF
[Unit]
Description=ClockBound

[Service]
Type=simple
Restart=always
RestartSec=10
ExecStart=/usr/local/bin/clockbound --max-drift-rate 50
RuntimeDirectory=clockbound
RuntimeDirectoryPreserve=yes
WorkingDirectory=/run/clockbound
User=_chrony
Group=_chrony

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable clockbound
sudo systemctl start clockbound

cd clock-bound-ffi
~/.cargo/bin/cargo build --release
cd ..
sudo cp clock-bound-ffi/include/clockbound.h /usr/include/
sudo cp target/release/libclockbound.a /usr/lib/
sudo cp target/release/libclockbound.so /usr/lib/
cd examples/c/src/
gcc clockbound_now.c -o clockbound_now -I/usr/include -L/usr/lib -lclockbound

sudo ls -l /var/run/clockbound/shm
./clockbound_now
````

Example clockbound_now output showing the server is correctly configured, the status is SYNCHRONIZED and the bounds are 28 microseconds apart:
```
When clockbound_now was called true time was somewhere within 1738356714.903589513 and 1738356714.903617449 seconds since Jan 1 1970. The clock status is SYNCHRONIZED.
It took 7.781244248 seconds to call clock bound 100000000 times (12851415 tps).
```

## LogCabin

Only needed on the client, since we'll share it over sshfs with the servers, below.

```
cd
git clone git@github.com:<ANONYMIZED>/logcabin.git
cd logcabin
git checkout --track origin/leaseguard
git submodule update --init
scons
sudo scons install
# LogCabin's "scons install" doesn't install client dev files
sudo cp -r include/LogCabin /usr/include
sudo cp build/liblogcabin.a /usr/lib
```

# sshfs

You want file changes on the client node to be synced to the servers. It's convenient when you make LogCabin code changes and recompile. The benchmarks rely on this: they write config files locally at the start of each run and expect the config files to appear on the servers moments later.

On each server (not the client):

```
ssh -i ~/.ssh/MY-PRIVATE-KEY.pem -o BatchMode=yes -o StrictHostKeyChecking=no ubuntu@CLIENT-INSTANCE.amazonaws.com exit # test ssh
sudo apt install -y sshfs
mkdir logcabin
sshfs -o IdentityFile=~/.ssh/MY-PRIVATE-KEY.pem -o StrictHostKeyChecking=no ubuntu@CLIENT-INSTANCE.amazonaws.com:/home/ubuntu/logcabin  /home/ubuntu/logcabin
```

Now in ~/logcabin on the servers you should see the same files as on the client machine.

After each reboot:

```
# restart sshfs
cd
killall sshfs
rm logcabin/*.out
sshfs -o IdentityFile=~/.ssh/MY-PRIVATE-KEY.pem -o StrictHostKeyChecking=no ubuntu@CLIENT-INSTANCE.amazonaws.com:/home/ubuntu/logcabin  /home/ubuntu/logcabin
```

## Experiments

To reproduce the charts in the LeaseGuard paper (this will take several days):

```
python3 leaseguard_experiments/network_latency_experiment.py --servers=server1,server2,server3 --trials 3
python3 leaseguard_experiments/unavailability_experiment.py --servers=server1,server2,server3 
python3 leaseguard_experiments/latency_vs_throughput_experiment.py --servers=server1,server2,server3 --trials 10
python3 make_charts.py
```

Replace "server1,server2,server3" with three servers which the Python scripts can control via passwordless ssh.

LogCabin's original README follows.

[![logo](logo/500px.png?raw=true)](logo/logo.svg)

Overview
========

LogCabin is a distributed system that provides a small amount of highly
replicated, consistent storage. It is a reliable place for other distributed
systems to store their core metadata and is helpful in solving cluster
management issues. LogCabin uses the
[Raft consensus algorithm](https://raft.github.io) internally and is actually
the very first implementation of Raft. It's released under the
[ISC license](https://en.wikipedia.org/wiki/ISC_license) (equivalent to BSD).

External resources:
- [Slide deck](https://logcabin.github.io/talk/)
  on LogCabin's usage, operations, and internals
- [Code-level documentation](https://logcabin.github.io/doxygen/annotated.html)
  built with Doxygen
- Recent updates on LogCabin's development on
  [Diego's blog](http://ongardie.net/blog/+logcabin/)

Information about releases is in [RELEASES.md](RELEASES.md).

This README will walk you through how to compile and run LogCabin.

Questions
=========

The best place to ask questions about the LogCabin implementation is on the
[logcabin-dev](https://groups.google.com/forum/#!forum/logcabin-dev) mailing
list. You might also try `#logcabin` on the freenode IRC network, although
there aren't always people around. Use GitHub Issues to report problems or
suggest features.

For questions and discussion about the Raft consensus algorithm, which LogCabin
implements, use the
[raft-dev](https://groups.google.com/forum/#!forum/raft-dev) mailing list.

Building
========

[![Build Status](https://travis-ci.org/logcabin/logcabin.svg?branch=master)](https://travis-ci.org/logcabin/logcabin)

Pre-requisites:

- Linux x86-64 (v2.6.32 and up should work)
- git (v1.7 and up should work)
- scons (v2.0 and v2.3 are known to work)
- g++ (v4.4 through v4.9 and v5.1 are known to work) or
  clang (v3.4 through v3.7 are known to work with libstdc++ 4.9, and libc++ is
  also supported; see [CLANG.md](CLANG.md) for more info)
- protobuf (v2.6.x suggested, v2.5.x should work, v2.3.x is not supported)
- crypto++ (v5.6.1 is known to work)
- doxygen (optional; v1.8.8 is known to work)

In short, RHEL/CentOS 6 should work, as well as anything more recent.

First build the [clock-bound](https://github.com/aws/clock-bound/) library and
install the lib and header file in the standard locations.

Get the source code:

    git clone git://github.com/logcabin/logcabin.git
    cd logcabin
    git submodule update --init


Build the client library, server binary, and unit tests:

    scons

For custom build environments, you can place your configuration variables in
`Local.sc`. For example, that file might look like:

    BUILDTYPE='DEBUG'
    CXXFLAGS=['-Wno-error']

To see which configuration parameters are available, run:

    scons --help

Running basic tests
===================

It's a good idea to run the included unit tests before proceeding:

    build/test/test

You can also run some system-wide tests. This first command runs the smoke
tests against an in-memory database that is embedded into the LogCabin client
(no servers are involved):

    build/Examples/SmokeTest --mock && echo 'Smoke test completed successfully'

To run the same smoke test against a real LogCabin cluster will take some more
setup.

Running a real cluster
======================

This section shows you how to run the `HelloWorld` example program against a
three-server LogCabin cluster. We'll run all the servers on localhost for now:

 - Server 1 will listen on 127.0.0.1:5254
 - Server 2 will listen on 127.0.0.1:5255
 - Server 3 will listen on 127.0.0.1:5256

Port 5254 is LogCabin's default port and is reserved by IANA for LogCabin. The
other two belong to others and are hopefully not in use on your network.

We'll first need to create three configuration files. You can base yours off of
sample.conf, or the following will work for now:

File `logcabin-1.conf`:

    serverId = 1
    listenAddresses = 127.0.0.1:5254

File `logcabin-2.conf`:

    serverId = 2
    listenAddresses = 127.0.0.1:5255

File `logcabin-3.conf`:

    serverId = 3
    listenAddresses = 127.0.0.1:5256

Now you're almost ready to start the servers. First, initialize one of the
server's logs with a cluster membership configuration that contains just
itself:

    build/LogCabin --config logcabin-1.conf --bootstrap

The server with ID 1 will now have a valid cluster membership configuration in
its log. At this point, there's only 1 server in the cluster, so only 1 vote is
needed: it'll be able to elect itself leader and commit new entries. We can now
start this server (leave it running):

    build/LogCabin --config logcabin-1.conf

We don't want to stop here, though, because the cluster isn't fault-tolerant
with just one server! We're going to start two more servers and then add them
both to the first server's cluster.

Let's start up the second server in another terminal (leave it running):

    build/LogCabin --config logcabin-2.conf

Note how this server is just idling, awaiting a cluster membership
configuration. It's still not part of the cluster.

Start the third server also (LogCabin checks to make sure all the servers in
your new configuration are available before committing to switch to it, just to
keep you from doing anything stupid):

    build/LogCabin --config logcabin-3.conf

Now use the reconfiguration command to add the second and third servers to the
cluster:

    ALLSERVERS=127.0.0.1:5254,127.0.0.1:5255,127.0.0.1:5256
    build/Examples/Reconfigure --cluster=$ALLSERVERS set 127.0.0.1:5254 127.0.0.1:5255 127.0.0.1:5256

This `Reconfigure` command is a special LogCabin client. It first queries each
of the servers given in its positional command line arguments (space-delimited
after the command "set") to retrieve their server IDs and listening addresses
(as set in their configuration files). Then, it connects to the cluster given
by the `--cluster` option (comma-delimited) and asks the leader to set the
cluster membership to consist of those servers. Note that the existing cluster
members should be included in the positional arguments if they are to remain in
the cluster; otherwise, they will be evicted from the cluster.

If this succeeded, you should see that the first server has added the others to
the cluster, and the second and third servers are now participating. It should
have output something like:

    Current configuration:
    Configuration 1:
    - 1: 127.0.0.1:5254

    Attempting to change cluster membership to the following:
    1: 127.0.0.1:5254 (given as 127.0.0.1:5254)
    2: 127.0.0.1:5255 (given as 127.0.0.1:5255)
    3: 127.0.0.1:5256 (given as 127.0.0.1:5256)

    Membership change result: OK

    Current configuration:
    Configuration 4:
    - 1: 127.0.0.1:5254
    - 2: 127.0.0.1:5255
    - 3: 127.0.0.1:5256

Note: If you're sharing a single magnetic disk under heavy load for all the
servers, the cluster may have trouble maintaining a leader. See
[issue 57](https://github.com/logcabin/logcabin/issues/57) for more details on
symptoms and a workaround.

Finally, you can run a LogCabin client to exercise the cluster:

    build/Examples/HelloWorld --cluster=$ALLSERVERS

That program doesn't do anything very interesting. Another tool called
TreeOps exposes LogCabin's data structure on the command line:

    echo -n hello | build/Examples/TreeOps --cluster=$ALLSERVERS write /world
    build/Examples/TreeOps --cluster=$ALLSERVERS dump

See the --help for a complete listing of the available commands.

You should be able to kill one server at a time and maintain availability, or
kill more and restart them and maintain safety (with an availability hiccup).

If you find it annoying to pass --cluster=$ALLSERVERS everywhere, you can also
use a DNS name to return all the IP addresses. However, you will need distinct
IP addresses for each server, not just distinct ports.

If you have your own application, you can link it against
`build/liblogcabin.a`. You'll also need to link against the following
libraries:

- pthread
- protobuf
- cryptopp

Running cluster-wide tests
==========================

The procedure described above for running a cluster is fairly tedious when you
just want to run some tests and tear everything down again. Thus,
`scripts/smoketest.py` automates it. Create a file called `scripts/localconfig.py`
to override the `smokehosts` and `hosts` variables found in `scripts/config.py`:

    smokehosts = hosts = [
      ('192.168.2.1', '192.168.2.1', 1, 5254),
      ('192.168.2.2', '192.168.2.2', 2, 5254),
      ('192.168.2.3', '192.168.2.3', 3, 5254),
    ]

The scripts use this file to when launching servers using SSH. Each tuple in
the (smoke)hosts list represents one server, containing:

 1. the address to use for SSH,
 2. the address to use for LogCabin TCP connections,
 3. a unique ID,
 4. a port number.

Each of these servers should be accessible over SSH without a password and
should have the LogCabin directory available in the same filesystem location.
The script currently assumes this directory to be on a shared filesystem, such
as an NFS mount or localhost. If the address to use for SSH is `localhost`,
make sure that `ssh localhost` works.

You may optionally create a `smoketest.conf` file, which can define
various options that apply to all the servers. The servers' listen addresses
will be merged with your `smoketest.conf` automatically.

Now you're ready to run:

    scripts/smoketest.py && echo 'Smoke test completed successfully'

This script can also be hijacked/included to run other test programs.

Documentation
=============

To build the documentation from the source code, run:

    scons docs

The resulting HTML files will be placed in `docs/doxygen`.

You can also find this documentation at <https://logcabin.github.io>.

Installation
============

To install a bunch of things on your filesystem, run:

    scons install

Along with the binaries, this installs a RHEL 6-compatible init script.

If you don't want these files to pollute your filesystem, you can install the files
to any given directory as follows (replace `pathtoinstallprefix` in both places
with wherever you'd like the files to go):

    scons --install-sandbox=pathtoinstallprefix pathtoinstallprefix

Finally, you can build a binary RPM as follows:

    scons rpm

This creates a file called `build/logcabin-0.0.1-0.1.alpha.0.x86_64.rpm` or
similar that you can then install using RPM, with the same effect as `scons
install`.

Contributing
============

Please use GitHub to report issues and send pull requests.

All commits should pass the pre-commit hooks. Enable them to run before each
commit:

    ln -s ../../hooks/pre-commit .git/hooks/pre-commit

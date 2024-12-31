#!/bin/sh

# Tries to follow the steps of README.md closely, which helps to keep that file
# up-to-date.

set -ex

killall -9 LogCabin Reconfigure || true

rm -rf storage debug
mkdir -p storage debug

cat >logcabin-1.conf << EOF
serverId = 1
listenAddresses = 127.0.0.1:5254
storagePath=storage
EOF

cat >logcabin-2.conf << EOF
serverId = 2
listenAddresses = 127.0.0.1:5255
storagePath=storage
EOF

cat >logcabin-3.conf << EOF
serverId = 3
listenAddresses = 127.0.0.1:5256
storagePath=storage
EOF


build/LogCabin --config logcabin-1.conf --bootstrap

build/LogCabin --config logcabin-1.conf --log debug/1 &

build/LogCabin --config logcabin-2.conf --log debug/2 &

build/LogCabin --config logcabin-3.conf --log debug/3 &

ALLSERVERS=127.0.0.1:5254,127.0.0.1:5255,127.0.0.1:5256
build/Examples/Reconfigure --cluster=$ALLSERVERS set 127.0.0.1:5254 127.0.0.1:5255 127.0.0.1:5256

build/Examples/HelloWorld --cluster=$ALLSERVERS

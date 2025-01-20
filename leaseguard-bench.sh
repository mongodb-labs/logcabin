#!/bin/sh

set -ex

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <server1> <server2> <server3>"
    exit 1
fi

SERVER1="$1"
SERVER2="$2"
SERVER3="$3"

cat <<EOF > conf1.conf
serverId = 1
listenAddresses = $SERVER1
clusterUUID = foo
storagePath = storage1
logPolicy = NOTICE
snapshotMinLogSize = 99999999999
EOF

cat <<EOF > conf2.conf
serverId = 2
listenAddresses = $SERVER2
clusterUUID = foo
storagePath = storage2
logPolicy = NOTICE
snapshotMinLogSize = 99999999999
EOF

cat <<EOF > conf3.conf
serverId = 3
listenAddresses = $SERVER3
clusterUUID = foo
storagePath = storage3
logPolicy = NOTICE
snapshotMinLogSize = 99999999999
EOF

sleep 5

echo 'SERVER 1 ================================'

ssh -v -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER1 <<EOF
set +x
cd logcabin
killall -9 perf LogCabin Reconfigure || true
rm -rf storage1 1.log
./build/LogCabin --config conf1.conf --bootstrap
echo DONE BOOTSTRAPPING
#nohup perf record -o perf1.data --call-graph=dwarf ./build/LogCabin --config conf1.conf --log 1.log >1.out 2>&1 </dev/null & 
nohup ./build/LogCabin --config conf1.conf --log 1.log >1.out 2>&1 </dev/null & 
ps aux | grep LogCabin
EOF

echo 'SERVER 2 ================================'

ssh -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER2 <<EOF
set +x
cd logcabin
killall -9 perf LogCabin Reconfigure || true
rm -rf storage2 2.log
#nohup perf record -o perf2.data --call-graph=dwarf ./build/LogCabin --config conf2.conf --log 2.log >2.out 2>&1 </dev/null & 
nohup ./build/LogCabin --config conf2.conf --log 2.log >2.out 2>&1 </dev/null & 
ps aux | grep LogCabin
EOF

echo 'SERVER 3 ================================'

ssh -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER3 <<EOF
set +x
cd logcabin
killall -9 perf LogCabin Reconfigure || true
rm -rf storage3 3.log
#nohup perf record -o perf3.data --call-graph=dwarf ./build/LogCabin --config conf3.conf --log 3.log >3.out 2>&1 </dev/null & 
nohup ./build/LogCabin --config conf3.conf --log 3.log >3.out 2>&1 </dev/null & 
ps aux | grep LogCabin
EOF

sleep 5

echo 'RECONFIGURE =============================='

build/Examples/Reconfigure --cluster=$SERVER1 set $SERVER1 $SERVER2 $SERVER3

echo 'HELLOWORLD ==============================='
build/Examples/HelloWorld --cluster=$SERVER1,$SERVER2,$SERVER3

./build/Examples/Benchmark --cluster=$SERVER1,$SERVER2,$SERVER3 --size=1024 --threads=100 --operation-type=read --timeout=30s --operations=100000

ssh -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER1 <<EOF
killall -9 perf LogCabin Reconfigure || true
EOF

ssh -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER2 <<EOF
killall -9 perf LogCabin Reconfigure || true
EOF

ssh -o StrictHostKeyChecking=no -i ~/.ssh/jesse-2024.pem $SERVER3 <<EOF
killall -9 perf LogCabin Reconfigure || true
EOF

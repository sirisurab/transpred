#!/bin/bash

ROOT=''
BLOCK=60000000
X=0

rm $ROOT/traffic_speed.part*

echo "spawning traffic data threads"

for i in {1..99}; do
    curl --range $X-$(( X + BLOCK - 1 )) -o $ROOT/traffic_speed.part$i  https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+* &
    echo "spawned traffic cab data thread "
    (( X = X + BLOCK ))

done

curl --range $X- -o $ROOT/traffic_speed.part100  https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+* &
echo "spawned traffic cab data thread 100"

wait

echo "merging traffic data"

cat $ROOT/traffic_speed.part* > $ROOT/traffic_speed.csv

rm $ROOT/traffic_speed.part*
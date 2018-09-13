#!/bin/bash

ROOT='/media/sb/springboard/TransPred/data/traffic'
BLOCK=60000000
X=0

rm $ROOT/traffic_speed.part*

for i in {1..99}; do
	curl --range $X-$(( X + BLOCK - 1 )) -o $ROOT/traffic_speed.part$i  https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*
	(( X = X + BLOCK ))

done

curl --range $X- -o $ROOT/traffic_speed.part100  https://data.cityofnewyork.us/api/views/i4gi-tjb9/rows.csv?accessType=DOWNLOAD&bom=true&query=select+*

wait

cat $ROOT/traffic_speed.part* > $ROOT/traffic_speed.csv

rm $ROOT/traffic_speed.part*

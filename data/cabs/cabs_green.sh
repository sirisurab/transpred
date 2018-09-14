#!/bin/bash

echo "spawning green cab curl processes"

for i in {1..2};do
    if [ $i -lt 10 ]
    then
        j="0$i"
    else
        j="$i"
    fi
    curl -O -v https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_[2016-2017]-$j.csv &
    echo "spawned green cab data thread "
done

wait

echo "merging green cab data"

awk '
    FNR==1 && NR!=1 { while (/^<header>/) getline; }
    1 {print}
    ' green_tripdata_*.csv > all_green_1617.csv

rm green_tripdata_*.csv
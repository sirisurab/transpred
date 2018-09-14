#!/bin/bash
echo "spawning yellow cab curl processes"

for i in {1..2};do
    if [ $i -lt 10 ]
    then
        j="0$i"
    else
        j="$i"
    fi

    curl -O -v https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_[2016-2017]-$j.csv &
    echo "spawned yellow cab data thread "

done

wait

echo "merging yellow cab data"

awk '
    FNR==1 && NR!=1 { while (/^<header>/) getline; }
    1 {print}
    ' yellow_tripdata_*.csv > all_yellow_1617.csv

rm yellow_tripdata_*.csv

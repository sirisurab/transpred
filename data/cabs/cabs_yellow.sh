#!/bin/bash


for i in {1..2};do
	if [ $i -lt 10 ]
	then
		j="0$i"
	else
		j="$i"
	fi
	curl -O	https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_[2016-2017]-$j.csv &

done

awk '
	FNR==1 && NR!=1 { while (/^<header>/) getline; }
	1 {print}
' yellow_tripdata_*.csv > all_yellow_1617.csv

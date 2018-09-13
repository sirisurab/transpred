#!/bin/bash


for i in {1..2};do
	if [ $i -lt 10 ]
	then
		j="0$i"
	else
		j="$i"
	fi
	curl -O	https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_[2016-2017]-$j.csv &

done

awk '
	FNR==1 && NR!=1 { while (/^<header>/) getline; }
	1 {print}
' green_tripdata_*.csv > all_green_1617.csv

#!/bin/bash

rm turnstile_*.txt

for i in {1..2};do
	if [ $i -lt 10 ]
	then
		j="0$i"
	else
		j="$i"
	fi	
	curl -O --fail  http://web.mta.info/developers/data/nyct/turnstile/turnstile_[16-17]$j[0-31].txt &
		

done

wait

awk '
	FNR==1 && NR!=1 { while (/^<header>/) getline; }
	1 {print}
' turnstile_*.txt > all_turnstile_1617.txt 

rm turnstile_*.txt

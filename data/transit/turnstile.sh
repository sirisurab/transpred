#!/bin/bash

rm turnstile_*.txt

echo "spawning turnstile data threads"

for i in {1..2};do
    if [ $i -lt 10 ]
    then
        j="0$i"
    else
        j="$i"
    fi

    curl -O --fail  http://web.mta.info/developers/data/nyct/turnstile/turnstile_[16-17]$j[0-31].txt &
    echo "spawned turnstile data thread"

done

wait

echo "merging turnstile data"

awk '
    FNR==1 && NR!=1 { while (/^<header>/) getline; }
    1 {print}
' turnstile_*.txt > all_turnstile_1617.txt

rm turnstile_*.txt
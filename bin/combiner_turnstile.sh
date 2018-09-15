#!/bin/bash

# inputs
# years (array)
# assume block_size is 1 (1 month data fetched by each fetcher as a block)
# each month is a separate file
years=("$@")

cd ../data/transit
echo "years $years"

# redis
# TODO move this to config file
redis_url="127.0.0.1"
redis_cli="redis-cli -h $redis_url"
q1="ts_q"
q2="ts_p"


# initialize blocks
# create empty file block_queue.txt in directory cabs
#rm block_queue
#touch block_queue
echo "DEL $q1" | ${redis_cli}
echo "DEL $q2" | ${redis_cli}

# for each year in years
# write 12 blocks (each in separate line)
for i in "${years[@]}";do

    year="${i:2:2}"

    for j in {1..12};do
        if [ $j -lt 10 ]
        then
            month="0$j"
        else
            month="$j"
        fi
        echo "pushing $q1 \"$year $month\""
        echo "LPUSH $q1 \"$year $month\"" | ${redis_cli}
    done

done

# poll blocks to check if all are complete (block_queue is empty again)
# if all complete then combine else keep polling
while true; do

    len_q=$(echo "LLEN $q1" | ${redis_cli})
    len_p=$(echo "LLEN $q2" | ${redis_cli})
    if [ "$len_q" = 0 ] && [ "$len_p" = 0 ]
    then
        break
    else
        sleep 2
    fi

done


rm all_turnstile.txt

echo "merging turnstile data"

awk '
    FNR==1 && NR!=1 { while (/^<header>/) getline; }
    1 {print}
' turnstile_*.txt > all_turnstile.txt

rm turnstile_*.txt
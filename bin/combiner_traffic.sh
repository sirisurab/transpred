#!/bin/bash

# inputs
# years (array)
# assume block_size is 5 (5 parts of traffic file are fetched by each fetcher as a block)
# each month is a separate file

cd ../data/traffic

# redis
# TODO move this to config file
redis_url="127.0.0.1"
redis_cli="redis-cli -h $redis_url"
q1="tf_q"
q2="tf_p"
max_bl_num=20


# initialize blocks
# create empty file block_queue.txt in directory cabs
#rm block_queue
#touch block_queue
echo "DEL $q1" | ${redis_cli}
echo "DEL $q2" | ${redis_cli}


# write 12 blocks (each in separate line)
for i in $( seq 1 ${max_bl_num} );do

    echo "pushing $q1 $i"
    echo "LPUSH $q1 $i" | ${redis_cli}

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


rm traffic_speed.csv

echo "merging traffic data"

cat traffic_speed.part* > traffic_speed.csv

rm traffic_speed.part*
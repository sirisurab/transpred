#!/bin/bash
cd data/cabs

# redis
# TODO move this to config file
redis_url="redis"
redis_cli="redis-cli -c -h 192.168.254.68 -p 7001"
q1="{g_cabs}:q"
q2="{g_cabs}:p"

# check blocks (file block_queue in directory cabs)
# pick next block to fetch (pop first line of block_queue)
while true;do
    msg=($(echo "RPOPLPUSH $q1 $q2" | ${redis_cli}))
    if [[ ! -z "$msg" ]]
    then
        break
    fi
    sleep 2
done

year="${msg[0]}"
month_range="${msg[1]}"
# for each item in block (total items = 6)
# build url and fetch file (spawn curl in parallel)
# save file in cabs directory
echo "spawning green cab curl processes for ${msg[@]}"

echo $year","$month_range

curl -O -v https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_${year}-[${month_range}].csv &

echo "spawned green cab data threads for $msg"

wait

# block finished , remove from processing queue
echo "LREM $q2 1 \"${msg[@]}\"" | ${redis_cli}

echo "fetched block for ${msg[@]}"
#!/bin/bash

# inputs
# years (array)
# assume block_size is 5 (5 parts of traffic file are fetched by each fetcher as a block)
# each month is a separate file

cd data/traffic

# redis
# TODO move this to .env file
redis_prfx="redis-cluster-m-"
redis_port='7001'
redis_cli="redis-cli -c -h $redis_prfx$redis_port -p $redis_port"
#readonly NAME_PREFIX="redis-cluster-m-"
#readonly LOCAL_CONTAINER_ID=$(docker ps -f name="$NAME_PREFIX" -q | head -n 1)
#readonly LOCAL_PORT=$(docker inspect --format='{{index .Config.Labels "com.docker.swarm.service.name"}}' "$LOCAL_CONTAINER_ID" | sed 's|.*-||')
#host_node="$NAME_PREFIX$LOCAL_PORT"
#host_ip=$(docker service inspect -f '{{index .Endpoint.VirtualIPs 1).Addr}}' "$host_node" | sed 's|/.*||')
#redis_cli="docker exec -it \"$LOCAL_CONTAINER_ID\"\
#           redis-cli -c -h \"$host_ip\" -p \"$LOCAL_PORT\""
q1="{tf}:q"
q2="{tf}:p"
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
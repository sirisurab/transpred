#!/bin/bash

# inputs
# years (array)
# assume block_size is 3 (3 months data fetched by each fetcher as a block)
# each month is a separate file
years=("$@")

cd data/cabs
echo "years $years"

# redis
# TODO move this to config file
# cluster
#redis_prfx="redis-cluster-m-"
#redis_port='7001'
#redis_cli="redis-cli -c -h $redis_prfx$redis_port -p $redis_port"
# standalone
redis_host='redis'
redis_cli="redis-cli -h $redis_host -p 6379"
#readonly NAME_PREFIX="redis-cluster-m-"
#readonly LOCAL_CONTAINER_ID=$(docker ps -f name="$NAME_PREFIX" -q | head -n 1)
#readonly LOCAL_PORT=$(docker inspect --format='{{index .Config.Labels "com.docker.swarm.service.name"}}' "$LOCAL_CONTAINER_ID" | sed 's|.*-||')
#host_node="$NAME_PREFIX$LOCAL_PORT"
#host_ip=$(docker service inspect -f '{{index .Endpoint.VirtualIPs 1).Addr}}' "$host_node" | sed 's|/.*||')
#redis_cli="docker exec -it \"$LOCAL_CONTAINER_ID\"\
#           redis-cli -c -h \"$host_ip\" -p \"$LOCAL_PORT\""
q1="{y_cabs}:q"
q2="{y_cabs}:p"


# initialize blocks
# create empty file block_queue.txt in directory cabs
#rm block_queue
#touch block_queue
echo "DEL $q1" | ${redis_cli}
echo "DEL $q2" | ${redis_cli}

# for each year in years
# write 2 blocks (each in separate line)
# year, 1
# year, 2
for i in "${years[@]}";do
    #write year[i], 1
    #write year[i], 2
    echo "pushing $q1 \"$i 01-03\""
    echo "LPUSH $q1 \"$i 01-03\"" | ${redis_cli}
    echo "pushing $q1 \"$i 04-06\""
    echo "LPUSH $q1 \"$i 04-06\"" | ${redis_cli}
    echo "pushing $q1 \"$i 07-09\""
    echo "LPUSH $q1 \"$i 07-09\"" | ${redis_cli}
    echo "pushing $q1 \"$i 10-12\""
    echo "LPUSH $q1 \"$i 10-12\"" | ${redis_cli}

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


rm all_yellow.csv

# combine all cab files into output file
awk '
    FNR==1 && NR!=1 { while (/^<header>/) getline; }
    1 {print}
    ' yellow_tripdata_*.csv > all_yellow.csv

rm yellow_tripdata_*.csv
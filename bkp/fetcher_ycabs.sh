#!/bin/bash
cd data/cabs

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

curl -O -v https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_${year}-[${month_range}].csv &

echo "spawned green cab data threads for $msg"

wait

# block finished , remove from processing queue
echo "LREM $q2 1 \"${msg[@]}\"" | ${redis_cli}

echo "fetched block for ${msg[@]}"
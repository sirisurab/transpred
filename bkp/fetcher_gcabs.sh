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
q1="{g_cabs}:q"
q2="{g_cabs}:p"

# minio
bucket='tp/gcabs/'

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
echo "spawning green cab data threads for ${msg[@]}"

echo $year","$month_range

mkdir -p gcabs${year}${month_range}

cd gcabs${year}${month_range}

curl -O -s https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_${year}-[${month_range}].csv &

echo "spawned green cab data threads for ${msg[@]}"

wait

while true; do

    if mc config host add --insecure --debug tp http://minio:9000 minio minio123
    then
        echo "mc connected to minio service"
        break
    else
        echo "minio service not running. waiting to retry connection"
        sleep 2
    fi

done

# create minio bucket
#mc mb --ignore-existing --debug ${bucket}
#mkdir -p /data/${bucket} && wait

cd ..
mc cp --recursive --debug gcabs${year}${month_range}/ ${bucket}
#\cp -rf gcabs${year}${month_range}/* /data/${bucket}

wait

echo "pushed green cab data to minio for ${msg[@]}"

# block finished , remove from processing queue
echo "LREM $q2 1 \"${msg[@]}\"" | ${redis_cli}

rm -r gcabs${year}${month_range}

echo "fetched block for ${msg[@]}"

exit 0
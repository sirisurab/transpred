#!/bin/bash
if [ $? -ne 0 ] ; then
	docker network create \
	--driver=overlay \
	--attachable=true \
	--gateway=10.0.0.1 \
	--subnet=10.0.0.0/24 \
	backend && \
	echo "created tp overlay network backend" 
fi

docker image inspect redis:alpine 2>/dev/null
if [ $? -ne 0 ] ; then
	docker pull redis:alpine 
	echo "pulled redis:alpine image"
fi

docker config inspect redis-config 2>/dev/null
if [ $? -ne 0 ] ; then
	docker config create redis-config $HOME/Transpred/config/redis.conf
	echo "created redis-config"
fi

docker service create \
  --name redis \
  --config source=redis-config,target=/usr/local/redis.conf \
  --mount type=bind,src=/mnt/redis/data,dst=/data \
  --network backend \ 
  redis:alpine redis-server /usr/local/redis.conf

exit 0

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

docker image inspect minio/minio 2>/dev/null
if [ $? -ne 0 ] ; then
	docker pull minio/minio 
	echo "pulled minio image"
fi
echo "minio" | docker secret create access-key-minio -
echo "minio123" | docker secret create secret-key-minio -
docker service create \
  -p 9000:9000 \
  --name minio \
  --secret=access-key-minio \
  --secret=secret-key-minio \
  --mount type=bind,src=/mnt/minio/data,dst=/data \
  --network backend \ 
  minio/minio server /data

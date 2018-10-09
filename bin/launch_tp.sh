#!/bin/bash

# ./start_backend_net.sh && \
docker system prune && \
cd Transpred && \
rm -Rf src && \
mkdir -p src && \
ssh-keyscan github.com > $HOME/.ssh/known_hosts && \
eval $(ssh-agent -s) && \
ssh-add $HOME/.ssh/id_rsa && \
git clone -v "ssh://git@github.com/sirisurab/transpred.git" src && \
docker pull redis:alpine && \
echo "pulled redis:alpine" && \
docker pull sirisurab/tp-app && \
echo "pulled tp-app:latest" && \
docker stack deploy --compose-file src/docker-compose.yml tp && \
echo "deployed to stack tp" && \
docker service ls

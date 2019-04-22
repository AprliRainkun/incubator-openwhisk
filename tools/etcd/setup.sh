#!/usr/bin/env bash

NODE1=$(hostname -I | cut -d " " -f1)
VOLUME_NAME="etcd-data"

REGISTRY="quay.io/coreos/etcd"

docker volume create --name ${VOLUME_NAME}

docker run -d \
    -p 2379:2379 \
    -p 2380:2380 \
    --volume=${VOLUME_NAME}:/etcd-data \
    --name etcd ${REGISTRY}:latest \
    /usr/local/bin/etcd \
    --data-dir=/etcd-data --name node1 \
    --initial-advertise-peer-urls http://${NODE1}:2380 \
    --listen-peer-urls http://0.0.0.0:2380 \
    --advertise-client-urls http://${NODE1}:2379 \
    --listen-client-urls http://0.0.0.0:2379 \
    --initial-cluster node1=http://${NODE1}:2380
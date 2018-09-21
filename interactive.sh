CONTAINER=$1
DC=tec1
docker rm -f $CONTAINER
docker pull docker.$DC.tivo.com/rvalsakumar/$CONTAINER:latest
docker run  -it --rm --entrypoint sh \
            --net host \
            --name $CONTAINER \
            --env CONTAINER_NAME=$CONTAINER \
            --env DATACENTER_NAME=$DC \
            --env ENVIRONMENT_NAME=rohit  \
            --env REGION_NAME=tivo \
            --env DEV_LOG_SINK=CONSOLE \
            --env KAFKA_ENDPOINT=kafka.$DC.tivo.com:9092 \
            --env ZOOKEEPER_ENDPOINT=zookeeper.$DC.tivo.com:2181 \
            --env DYNCONFIG_HOST=dynconfig.$DC.tivo.com \
            --env ANONYMIZER_HOST=anonymizer.$DC.tivo.com \
            -v /tmp/logs/$CONTAINER:/TivoData/Log \
            -v /tmp/data/$CONTAINER:/TivoData/$CONTAINER \
            docker.tivo.com/rvalsakumar/$CONTAINER

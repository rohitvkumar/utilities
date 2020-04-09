CONTAINER=$1
PORT=${2:-8080}
echo $PORT
LOC=${PWD}
DC=tec1
docker rm -f $CONTAINER
docker run  -it --rm --entrypoint "/home/tivo/$CONTAINER/bin/$CONTAINER" \
            --name $CONTAINER \
            --env CONTAINER_NAME=$CONTAINER \
            --env DATACENTER_NAME=$DC \
            --env ENVIRONMENT_NAME=rohit  \
            --env REGION_NAME=tivo \
            --env DEV_LOG_SINK=CONSOLE \
            --env DEV_LOG_LEVEL=debug \
            --env KAFKA_ENDPOINT=kafka.$DC.tivo.com:9092 \
            --env ZOOKEEPER_ENDPOINT=zookeeper.$DC.tivo.com:2181 \
            --env DYNCONFIG_ENDPOINT=dynconfig.$DC.tivo.com:50000 \
            --env TOKEN_ENDPOINT=core01.$DC.tivo.com:40017 \
            --env ANONYMIZER_HOST=anonymizer.$DC.tivo.com \
            --env HOSTNAME=laptop-rohit.$DC.tivo.com \
            --publish $PORT:$PORT \
            -v /tmp/logs/$CONTAINER:/TivoData/Log \
            -v /tmp/data/$CONTAINER:/TivoData/$CONTAINER \
            -v $LOC/tivo/bin:/TivoData/bin \
            docker.tivo.com/rvalsakumar/$CONTAINER:latest

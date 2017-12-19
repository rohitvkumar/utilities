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
            -v /TivoData/containers/logs/$CONTAINER:/TivoData/Log \
            -v /TivoData/containers/data/$CONTAINER:/TivoData/$CONTAINER \
            docker.$DC.tivo.com/rvalsakumar/$CONTAINER

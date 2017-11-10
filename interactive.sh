CONTAINER=$1
docker rm -f $CONTAINER
docker pull docker.tivo.com/rvalsakumar/$CONTAINER:latest
docker run  --rm -it --entrypoint sh \
            --net host \
            --name $CONTAINER \
            --env CONTAINER_NAME=$CONTAINER \
            --env DATACENTER_NAME=tec1 \
            --env ENVIRONMENT_NAME=rohit  \
            --env REGION_NAME=tivo \
            -v /TivoData/containers/logs/$CONTAINER:/TivoData/Log \
            -v /TivoData/containers/data/$CONTAINER:/TivoData/$CONTAINER \
            docker.tivo.com/rvalsakumar/$CONTAINER

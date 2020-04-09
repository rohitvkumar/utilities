#!/bin/bash
CTR=$1
docker rm -f $1_functional-tests
docker run --name $1_functional-tests -t --rm --net host -w /home/core/workspace/$1 -v /tmp/$1:/home/core/workspace/$1 docker.tivo.com/rvalsakumar/$1:latest functional-tests $1-usqe1-01.tea2.tivo.com
docker rm -f $1_functional-tests

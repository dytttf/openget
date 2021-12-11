#!/bin/bash
#
mkdir -p resources
cp ../dist/*.whl resources
#
docker rmi graper:0.0.1
docker build --no-cache -t graper:0.0.1 .
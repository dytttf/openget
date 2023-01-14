#!/bin/bash
#
mkdir -p resources
cp ../dist/*.whl resources
#
docker rmi openget:0.0.1
docker build --no-cache -t openget:0.0.1 .
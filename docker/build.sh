#!/bin/bash
#
version=$1

#
mkdir -p resources
cp ../dist/*.whl resources
#
docker rmi openget:$version
docker build --no-cache -t openget:$version .
#!/usr/bin/env bash

export CLASSPATH=mina-core-1.1.2.jar:mina-filter-compression-1.1.2.jar:slf4j-api-1.4.3.jar:slf4j-simple-1.4.3.jar:.
args="${@:-sim 3 localhost 9000}"
java edu.cmu.neuron2.RonTest $args

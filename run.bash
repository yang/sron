#!/usr/bin/env bash

export CLASSPATH=ext/mina-core-1.1.2.jar:ext/mina-filter-compression-1.1.2.jar:ext/slf4j-api-1.4.3.jar:ext/slf4j-simple-1.4.3.jar:.
args="${@:-sim 3 localhost 9000}"
java edu.cmu.neuron2.RonTest $args

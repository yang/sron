#!/usr/bin/env bash

if [ $# -lt 2 ]
then
        echo "Usage: ./run.bash neuron.properties numNodes"
        exit
fi

export CLASSPATH=ext/mina-core-1.1.2.jar:ext/mina-filter-compression-1.1.2.jar:ext/slf4j-api-1.4.3.jar:ext/slf4j-simple-1.4.3.jar:.
echo $CLASSPATH
java -Dneuron.config=$1 edu.cmu.neuron2.RonTest $2 & thepid=$!
sleep 60
kill $!

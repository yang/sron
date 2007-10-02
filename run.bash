#!/usr/bin/env bash
# vim:et:sw=2:ts=2

if [ -d ext ]
then base=ext
else base=.
fi

# if we're passing in the args, then don't load them from disk
if (( $# == 0 ))
then opts='-Dneuron.config=neuron.properties'
fi

# if [[ "$1" == delay ]]
# then delay=60; shift
# fi

export CLASSPATH=$base/mina-core-1.1.2.jar:$base/slf4j-api-1.4.3.jar:$base/slf4j-simple-1.4.3.jar:classes:scaleron.jar

exec java $opts "$@" edu.cmu.neuron2.RonTest

# if [[ $delay ]] ; then
#   pid=
#   function stop() {
#     if [[ $pid ]]
#     then kill $pid; pid=
#     fi
#     wait $pid
#   }
#   trap stop INT
#   java $opts "$@" edu.cmu.neuron2.RonTest &
#   pid=$!
#   sleep $delay
#   stop
# else
#   exec java $opts "$@" edu.cmu.neuron2.RonTest
# fi

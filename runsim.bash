#!/usr/bin/env bash

. common.bash

label=$1
shift
logdir=$SCALERON_ARCHIVES/$label/logs/ron
logbase=$logdir/long-log-
rm -rf $logdir
mkdir -p $logdir
./run.bash \
  -DnumNodes=47 \
  -Dmode=sim \
  -DlogFileBase=$logbase \
  -DtotalTime=$((5*60)) "$@" > $logdir/0 2>&1

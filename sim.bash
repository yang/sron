#!/usr/bin/env bash
# vim:et:sw=2:ts=2

set -o errexit
set -o nounset

basedir=.
datadir="$basedir/data"
failures={1..8}

case $1 in
  failure ) \
    schemes=simple sqrt
    num_nodes_range=

silence() {
  "$@" >& /dev/null || true
}

remkdir() {
  silence rm -r "$1"
  mkdir -p "$1"
}

if [[ "$1" == clean ]]
then echo cleaning; remkdir "$datadir"; shift
fi

for scheme in $schemes ; do # sqrt_special ; do
  for num_nodes in $num_nodes_range ; do
    for num_failures in $num_failures_range ; do
      for run in $runs ; do
        echo $scheme $num_nodes
        subdir="$datadir/$scheme/$num_nodes"
        remkdir "$subdir"
        ./run.bash \
            -DtotalTime=200 \
            -DlogFileBase="$subdir/" \
            -DconsoleLogFilter=all \
            -DnumNodes=$num_nodes \
            -Dscheme=$scheme \
            > /dev/null
      done
    done
  done
done

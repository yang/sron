#!/usr/bin/env bash
# vim:et:sw=2:ts=2

set -o errexit
set -o nounset

basedir=.
datadir="$basedir/data"

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

for scheme in simple sqrt ; do # sqrt_special ; do
  for numnodes in "$@" ; do
    echo $scheme $numnodes
    subdir="$datadir/$scheme/$numnodes"
    remkdir "$subdir"
    ./run.bash \
        -DtotalTime=60 \
        -DlogFileBase="$subdir/" \
        -DfileLogFilter='send.Ping recv.Ping send.Pong recv.Pong' \
        -DconsoleLogFilter=all \
        -DnumNodes=$numnodes \
        -Dscheme=$scheme \
        > /dev/null
  done
done

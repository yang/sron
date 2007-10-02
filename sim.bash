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

# remkdir "$datadir"

for scheme in simple sqrt sqrt_special ; do
  # for numnodes in {5..15} ; do # 4 9 16 25 36 49 64 81 100 ; do
  for numnodes in 80 100 ; do # $(seq 90 10 100) ; do
    echo $scheme $numnodes
    subdir="$datadir/$scheme/$numnodes"
    remkdir "$subdir"
    ./run.bash delay \
        -DlogFileBase="$subdir/" \
        -DfileLogFilter='send.Ping recv.Ping send.Pong recv.Pong' \
        -DconsoleLogFilter=all \
        -DnumNodes=$numnodes \
        -Dscheme=$scheme \
        > /dev/null
  done
done

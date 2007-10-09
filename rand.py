#!/usr/bin/env python
# vim:et:sw=2:ts=2

from random import *
from sys import *

percent = int(argv[1])
n = int(argv[2])
total_time = int(argv[3])

pairs = [ (i,j) for i in range(n) for j in range(n) ]
start = 47
end = total_time + 10

r = Random()
count = int( float( percent ) / 100 * n )
r.shuffle(pairs)
failures = [ (pair, start, end) for pair in pairs[:count] ]

for (i,j),s,t in failures:
  print i,j,s,t

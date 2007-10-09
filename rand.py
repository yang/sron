#!/usr/bin/env python
# vim:et:sw=2:ts=2

from random import *
from sys import *

frac = int(argv[1])
n = int(argv[2])

pairs = [ (i,j) for i in range(n) for j in range(n) ]
start = 47
end = 47

r = Random()
count = float( frac ) / n
head = r.shuffle(pairs)[:count]
failures = [ (pair, start, end) for pair in pairs ]

for (i,j),s,t in failures:
  print i,j,s,t

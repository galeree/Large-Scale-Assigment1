#!/usr/bin/env python

import sys

current_count = 0

for line in sys.stdin:
	line = line.strip()
	count, word = line.split('\t')
	
	if current_count < 100:
		print '%s\t%s' % (word, count)
		current_count += 1
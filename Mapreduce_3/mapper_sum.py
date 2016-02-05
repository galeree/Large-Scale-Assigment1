#!/usr/bin/env python

import sys

for line in sys.stdin:
	line = line.strip()
	filename, word, count = line.split('\t')
	print '%s\t%s' % (word, count)
#!/usr/bin/env python
#-*-coding: utf-8 -*-

import sys

for line in sys.stdin:
	line = line.strip()
	word, count, filename = line.split('\t')
	print '%s\t%s\t%s' % (filename, count, word)
#!/usr/bin/env python
#-*-coding: utf-8 -*-

import sys

current_filename = None
current_wordorder = 0
for line in sys.stdin:
	line = line.strip()
	filename, count, word = line.split('\t')
	if current_filename == filename:
		current_wordorder += 1
	else:
		current_wordorder = 1
	
	if current_wordorder <= 100:
		print '%s\t%s\t%s' % (filename, word, count)
	
	current_filename = filename
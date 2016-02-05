#!/usr/bin/env python

from operator import itemgetter
import sys

current_word = None
current_count = 0
word = None
filename = None

for line in sys.stdin:
	line = line.strip()
	word, count = line.split('\t',1)

	try:
		count = int(count)
	except ValueError:
		continue

	if current_word == word:
		current_count += count
	else:
		if current_word:
			key, filename = current_word.split(',')
			print '%s\t%s\t%s' % (key, current_count, filename)
			#print '%s\t%s\t%s' % (current_word, current_count, filename)
		current_count = count
		current_word = word

if current_word == word:
	#print '%s\t%s\t%s' % (current_word, current_count, filename)
	key, filename = current_word.split(',')
	print '%s\t%s\t%s' % (key, current_count, filename)
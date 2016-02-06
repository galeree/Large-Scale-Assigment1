#!/usr/bin/env python

import sys

def mapper():
	for line in sys.stdin:
		line = line.strip()
		word, count = line.split('\t')
		print '%s\t%s' % (count, word)

def reducer():
	current_count = 0

	for line in sys.stdin:
		line = line.strip()
		count, word = line.split('\t')
		
		if current_count < 100:
			print word
			current_count += 1

if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
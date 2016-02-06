#!/usr/bin/env python

import sys

def mapper():
	for line in sys.stdin:
		line = line.strip()
		word, count, filename = line.split('\t')
		print '%s\t%s\t%s' % (filename, count, word)

def reducer():
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

if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()


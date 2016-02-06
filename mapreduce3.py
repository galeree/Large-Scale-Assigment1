#!/usr/bin/env python

import sys

def mapper():
	for line in sys.stdin:
		line = line.strip()
		filename, word, count = line.split('\t')
		print '%s\t%s' % (word, count)

def reducer():
	current_word = None
	current_count = 0
	word = None
	filename = None

	for line in sys.stdin:
		line = line.strip()
		word, count = line.split('\t')

		try:
			count = int(count)
		except ValueError:
			continue

		if current_word == word:
			current_count += count
		else:
			if current_word:
				print '%s\t%s' % (current_word, current_count)
			current_count = count
			current_word = word

	if current_word == word:
		print '%s\t%s' % (current_word, current_count)

if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
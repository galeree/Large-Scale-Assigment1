#!/usr/bin/env python

import sys

def mapper():
	for line in sys.stdin:
		print line.strip()


def reducer():
	current_filename = None
	flag = 0
	attributes = []
	for line in sys.stdin:
		line = line.strip()
		filename, word, count = line.split('\t')
		if filename != current_filename and flag == 1:
			row = current_filename
			for attribute in attributes:
				row += '\t' + attribute
			print row
			attributes = []
		flag = 1
		attributes.append(count)
		current_filename = filename

	row = current_filename
	for attribute in attributes:
		row += '\t' + attribute
	print row

if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
#!/usr/bin/env python

import sys
import fileinput
from subprocess import Popen, PIPE

# Path to top100 word
def_dicts = []
cat = Popen(["hdfs", "dfs", "-cat", "/filter/part-00000"], stdout=PIPE)

for line in cat.stdout:
	def_dicts.append(line.strip())

cat.stdout.close()

def mapper():
	current_filename = None
	dicts = list(def_dicts)
	flag = 0
	for line in sys.stdin:
		line = line.strip()
		filename, word, count = line.split('\t')
		if filename != current_filename and flag == 1:
			for i in dicts:
				print '%s\t%s\t%s' % (current_filename, i, 0)
			dicts = list(def_dicts)
		if word in dicts:
			print '%s\t%s\t%s' % (filename, word, count)
			dicts.remove(word)
		flag = 1
		current_filename = filename

	for i in dicts:
		print '%s\t%s\t%s' % (current_filename, i, 0)


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
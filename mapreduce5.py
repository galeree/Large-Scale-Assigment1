#!/usr/bin/env python

import sys
import fileinput

# Path to top100 word
word_file = "/Users/Galle/Large-Scale-Assigment1/Result/result4.txt"
def_dicts = []

for line in fileinput.input([word_file]):
	def_dicts.append(line.strip())

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
	for line in sys.stdin:
		print line.strip()


if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
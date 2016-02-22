#!/usr/bin/env python

import sys

def mapper():
	def_dicts = []
	fo = open("part-00000", "r")
	for line in fo:
		def_dicts.append(line.strip())

	fo.close()
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
  current_key = None
  current_count = 0
  key = None

  for line in sys.stdin:
    line = line.strip()
    filename, word, count = line.split('\t')
    key = word + '\t' + filename
    try:
      count = int(count)
    except ValueError:
      continue

    if current_key == key:
      current_count += count
    else:
      if current_key:
        current_word, current_filename = current_key.split('\t')
        print '%s\t%s\t%s' % (current_filename, current_word, current_count)
      current_count = count
      current_key = key

  if current_key == key:
    current_word, current_filename = current_key.split('\t')
    print '%s\t%s\t%s' % (current_filename, current_word, current_count)


if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
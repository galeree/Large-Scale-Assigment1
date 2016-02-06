#!/usr/bin/env python
#-*-coding: utf-8 -*-

import sys
import os
import PyICU
from operator import itemgetter

def isThai(chr):
    cVal = ord(chr)
    if(cVal >= 3584 and cVal <= 3711):
        return True

    return False

def wrap(txt):
  txt = PyICU.UnicodeString(txt)
  bd = PyICU.BreakIterator.createWordInstance(PyICU.Locale("th"))
  bd.setText(txt)   
  lastPos = bd.first()
  retTxt = PyICU.UnicodeString("")
  txt_list = []
  try:
    while(1):
      currentPos = bd.next()
      retTxt += txt[lastPos:currentPos]

      txt_list.append(txt[lastPos:currentPos])
      if(isThai(txt[currentPos-1])):
        if(currentPos < len(txt)):
          if(isThai(txt[currentPos])):
            pass
      lastPos = currentPos
  except StopIteration:
    pass
  return [unicode(i) for i in txt_list]

def mapper():
  filename = os.environ['mapreduce_map_input_file']
  for line in sys.stdin:
  	line = line.strip()
  	items = line.split()
  	for item in items:
  		words = wrap(item)
  		for word in words:
  			print word.encode('utf-8')+','+filename+'\t'+str(1)

def reducer():
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
        current_key, current_filename = current_word.split(',')
        print '%s\t%s\t%s' % (current_key, current_count, current_filename)
      current_count = count
      current_word = word

  if current_word == word:
    current_key, current_filename = current_word.split(',')
    print '%s\t%s\t%s' % (current_key, current_count, current_filename)

if __name__ == '__main__':
  option = sys.argv[1]
  if option == "mapper":
      mapper()
  else:
      reducer()

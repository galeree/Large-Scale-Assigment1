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
        searchObj = re.search('[a-zA-Z]',word)
        if ~searchObj:
  			 print word.encode('utf-8')+'\t'+filename+'\t'+str(1)

def reducer():
  symbol = "~`!@#$%^&*()_-+={}[]:>;',</?*-+.ๆฯ"
  current_key = None
  current_count = 0
  key = None

  for line in sys.stdin:
    line = line.strip()
    word, filename, count = line.split('\t')
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
        if len(current_word) == 1:
          if current_word not in symbol:
            print '%s\t%s\t%s' % (current_word, current_count, current_filename)
        else:
          print '%s\t%s\t%s' % (current_word, current_count, current_filename)
      current_count = count
      current_key = key

  if current_key == key:
    current_word, current_filename = current_key.split('\t')
    if len(current_word) == 1:
      if current_word not in symbol:
        print '%s\t%s\t%s' % (current_word, current_count, current_filename)
    else:
      print '%s\t%s\t%s' % (current_word, current_count, current_filename)

if __name__ == '__main__':
  option = sys.argv[1]
  if option == "mapper":
      mapper()
  else:
      reducer()

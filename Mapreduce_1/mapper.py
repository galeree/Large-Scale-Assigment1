#!/usr/bin/env python
#-*-coding: utf-8 -*-

import sys
import os
import PyICU

filename = os.environ['mapreduce_map_input_file']

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
      #
      txt_list.append(txt[lastPos:currentPos])
      #Only thai language evaluated
      if(isThai(txt[currentPos-1])):
        if(currentPos < len(txt)):
          if(isThai(txt[currentPos])):
            #This is dummy word seperator   
            #retTxt += PyICU.UnicodeString("|||")
            #
            pass
      lastPos = currentPos
  except StopIteration:
    pass
    #retTxt = retTxt[:-1]
  #return retTxt
  return [unicode(i) for i in txt_list]

for line in sys.stdin:
	line = line.strip()
	items = line.split()
	for item in items:
		words = wrap(item)
		for word in words:
			print word.encode('utf-8')+','+filename+'\t'+str(1)
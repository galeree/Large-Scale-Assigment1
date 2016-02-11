#!/usr/bin/env python

centers = []
centers.append([1.0]*100)
centers.append([2.0]*100)
centers.append([3.0]*100)

fo = open("center.txt","wb")
for center in centers:
	row = ""
	for feature in center:
		row += str(feature)+'\t'
	row = row.strip()
	fo.write(row+'\n')

fo.close()
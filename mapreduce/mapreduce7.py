#!/usr/bin/env python

import sys
from subprocess import Popen, PIPE

def mapper(path):
	centers = []
	cat = Popen(["hdfs", "dfs", "-cat", path], stdout=PIPE)

	for line in cat.stdout:
		line = line.strip()
		line = line.split('\t')
		value = []
		for i in range(0,len(line)):
			value.append(float(line[i]))
		
		centers.append(value)

	cat.stdout.close()

	for line in sys.stdin:
		line = line.strip()
		line = line.split('\t')
		point = line[1:]
		min_diff = 10000000000
		cluster = 0
		for i in range(0,len(centers)):
			diff = 0
			center = centers[i]
			for j in range(0,len(center)):
				diff += ((float)(point[j])-(float)(center[j]))*((float)(point[j])-(float)(center[j]))

			if diff < min_diff:
				min_diff = diff
				cluster = i+1

			row = str(cluster)
			for j in range(0,len(point)):
				row += '\t'+str(point[j])
			print row

def reducer():
	for line in sys.stdin:
		line = line.strip()
		print line


if __name__ == '__main__':
	option = sys.argv[1]
	param = sys.argv[2]
	if option == "mapper":
   		mapper(param)
   	else:
   		reducer()
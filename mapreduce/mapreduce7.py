#!/usr/bin/env python

import sys

def mapper():
	centers = []
	fo = open("part-00000", "r")
	for line in fo:
		line = line.strip()
		line = line.split('\t')
		value = []
		for i in range(0,len(line)):
			value.append(float(line[i]))
		
		centers.append(value)
	fo.close()

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
	current_cluster = None
	cluster = None
	current_sum = [0]*100
	num_point = 0
	flag = 0
	for line in sys.stdin:
		line = line.strip()
		line = line.split('\t')
		cluster = (int)(line[0])
		point = line[1:]
		
		for i in range(0,len(point)):
			current_sum[i] += (float)(point[i])

		if cluster != current_cluster and flag == 1:
			row = ""
			for i in range(0,len(current_sum)):
				row += (str)((float)(current_sum[i]/num_point)) + '\t'
			print row.strip()
			num_point = 0
			current_sum = [0]*100

		current_cluster = cluster
		num_point += 1
		flag = 1

	row = ""
	for i in range(0,len(current_sum)):
		row += (str)((float)(current_sum[i]/num_point)) + '\t'
	print row.strip()


if __name__ == '__main__':
	option = sys.argv[1]
	if option == "mapper":
   		mapper()
   	else:
   		reducer()
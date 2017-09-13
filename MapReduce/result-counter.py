#!/usr/bin/env python

'''
 This package is specific to counting the
 final result . Suppose total sales , and total value
 in sales.
'''

import sys 

def main():

	total_value = 0

	for line in sys.stdin:
		data = line.strip().split("\t")
		
		# x and y are variable which can be anything
		# which you want to calculate
		x , y = data 		

		if len(data)!= 2:
			continue

		total_value += float(y)

	print "Total Value\t"+str(total_value)


if __name__ == "__main__":
	main()
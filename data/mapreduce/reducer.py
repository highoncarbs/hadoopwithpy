#!/usr/bin/env python

import sys 

def main():
	# Following the mapper.py , we need to get total
	# sales based on category.

	salesTotal = 0
	oldKey = None 

	for line in sys.stdin:
		
		data = line.strip().split("\t")

		# Our mapper gives out 2 values

		if len(data) != 2:
			# This keeps check of the data errors
			continue

		thisKey , thisSale = data 
		if oldKey and oldKey != thiskey:
			# The if statement makes sure to get 
			# sales only of that key 

			print oldKey,"\t",salesTotal
			oldKey = thisKey
			# reset the Total Sales for the next category(key)
			salesTotal = 0 

		oldKey = thisKey
		salesTotal += float(thisSale)

	if oldKey != None:
		print oldKey , "\t",salesTotal


# Calling the main function

main()
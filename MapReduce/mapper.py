#!/usr/bin/env python

import sys

def mapper():
	# Read input line
	for line in sys.stdin:
		# Strip off whitespace , and split on tab
		data = line.strip().split('\t')

		# We'll use the data stored in data/mapreduce/
		# Having six columns of tab seperated values
		# We'll make sure that correct data in sent through
		print data
		if len(data) == 6:
			date , time , store , category ,cost , payment = data

			# Now we'll print our data as required for the reducer task
			# I need category and cost so we'll print that
			print "{0}\t{1}".format(store , cost)

mapper()

'''

# Testing :

test_data = """2012-01-01\t09:00\tSan Jose\tMen's Clothing\t214.05\tAmex\n
	2012-01-01\t09:00\tFort Worth\tWomen's Clothing\t153.57\tVisa\n
	2012-01-01\t09:00\tSan Diego\tMusic\t66.08\tCash\n
	2012-01-01\t09:00\tPittsburgh\tPet Supplies\t493.51\tDiscover\n
	2012-01-01\t09:00\tOmaha\tChildren's Clothing\t235.63\tMasterCard"""

def main():

	# Used for testing the mapper function

	import StringIO
	sys.stdin = StringIO.StringIO(test_data)
	mapper()
	sys.stdin = sys.__stdin__

'''

#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This follows the below sequence.
Data set used is MoviLens small dataset

Mapreduce work :

Mapper 1 :  Extract user, (movie,rating)
Input – > u.data
output – >  userID, (movieID, rating)

Reducer 1 : Group, user, (movie,rating)
Input –> userID, (movieID, rating)
output –> userID, [(movieID,rating)(movieID,rating)…]

Mapper 2 :  Create movie, rating pairs for each user (combinations)
Input – > userID, [(movieID,ratings) (movieID,ratings)…]
output -> (movieID,movieID) , (rating, rating)

Reducer 2 :  Compute rating based similarity for each movie pair (Cosine Similarity)
Input – > (movieID, movieID), ([(rating,rating)(rating,rating)…]
Output – > (movieID, movieID), (Similarity Score, Number of users who saw both)

Mapper 3 :  Get movie Name and Sort based on similarity Score
Reducer 3 : Final result displayed grouped by movie name

'''

import sys
import ast
def reducer():

    oldKey = None
    rating_arr = []

    for line in sys.stdin:
        # So we'll recieve user, (movie,rating)
        # We need to group the tuples for unique users
        # we'll append the tuples to an array
        # Given that we have two data points , we'll split the
        # data at only first occurance of ','
        # This splits the string only at first comma

        data = line.strip().split(',',1)
        # print len(data) , data
        # check for 2 data values
        if len(data) != 2:
            continue

        x , y = data

        if oldKey and oldKey != x:

            print "{0},{1}".format(oldKey , rating_arr)
            oldKey = x
            rating_arr = []
        oldKey = x
        rating_arr.append(ast.literal_eval(y))
        # print rating_arr
    if oldKey != None:
        print "{0},{1}".format(oldKey , rating_arr)


# '''
# Testbed

test_data= """671,(4973,4.5)\n671,(4993,5.0)\n671,(4995,4.0)\n672,(4195,2.0)\n672,(4975,4.0)"""

def main():

	# Used for testing the mapper function

	import StringIO
	sys.stdin = StringIO.StringIO(test_data)
	reducer()
	sys.stdin = sys.__stdin__

main()
# '''

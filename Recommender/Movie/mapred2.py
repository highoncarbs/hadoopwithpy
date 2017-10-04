#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This reducer computes the similarity between movies using ratings.
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

from __future__ import print_function
import sys
from math import sqrt
import ast

def simplify():
    '''
    Takes the mapper2 output and simplifies the data.
    Same pair gets one list of ratings.
    INPUT : (movieid , movieid)(rating,rating)
    OUTPUT: (movieid , movieid)[(rating,rating),(rating , rating)]
    '''
    set = {}
    for line in sys.stdin:
        data = ast.literal_eval(line)
        movie_pair , rating_pair = data
        if movie_pair not in set.keys():
            set[movie_pair] = [rating_pair]
        if movie_pair in set.keys():
            set[movie_pair].append(rating_pair)
    for key in set.keys():
        print("{0},{1}".format(key, set[key]))

# '''
# Testbed

test_data= """(4973, 4993),(4.5, 5.0)
(4973, 4993),(3.5, 3.0)
(4973, 4993),(4.0, 4.0)
(4973, 4995),(4.5, 4.0)
(4993, 4995),(5.0, 4.0)
(4195, 4975),(2.0, 4.0)
(4195, 4995),(2.0, 4.0)
(4975, 4995),(4.0, 4.0)
"""

def main():

    # Used for testing the mapper function

    import StringIO
    sys.stdin = StringIO.StringIO(test_data)
    simplify()
    sys.stdin = sys.__stdin__

main()
# '''

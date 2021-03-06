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
Inital – > Mapred2 applies set on output of Mapper2 and gives values required by Input
Input – > (movieID, movieID), ([(rating,rating)(rating,rating)…]
Output – > (movieID, movieID), (Similarity Score, Number of users who saw both)

Mapper 3 :  Get movie Name and Sort based on similarity Score
Reducer 3 : Final result displayed grouped by movie name

'''
from __future__ import print_function
import sys
from math import sqrt
import ast

def reducer():
    '''
    Calculates cosine similarity between two movie given
    movieid and its ratings
    for more info on cosine similarity refer to this:
    http://blog.christianperone.com/2013/09/machine-learning-cosine-similarity-for-vector-space-models-part-iii/

    INPUT:
    (movieid , movieid),(rating1 , rating2)
    OUTPUT :
    (movieid , movieid),(similarity , count)
    '''

    for line in sys.stdin:
        # Need to check the input
        # Accoring to flow , this needs a moviepair and ist of ratings

        line = ast.literal_eval(line)
        movie_pair , rating_pair = line
        score , num_pair = cosine(rating_pair)

        if (num_pair>0 and score > 0.95):
            print("{0},{1}".format(movie_pair , (score , num_pair)))

def cosine(rating_pair):
    '''
    Calculates inv(cosine) of two numbers
    FORMULA: a*b/sqrt(a^2)*sqrt(b^2)

    INPUT: tuple(a,b)
    OUTPUT: score , num_pair
    '''

    score = 0
    num_pair = 0
    sum_aa , sum_ab , sum_bb = (0,0,0)
    for a,b in rating_pair:
        sum_aa += a*a
        sum_ab += a*b
        sum_bb += b*b
        num_pair += 1

    numerator = sum_ab
    denominator = sqrt(sum_aa) * sqrt(sum_bb)

    if denominator != 0 :
        score = numerator/denominator

    return(score , num_pair)

# '''
# Testbed

test_data= """(4973, 4993),[(4.5, 5.0), (4.5, 5.0), (3.5, 3.0), (4.0, 4.0)]
(4993, 4995),[(5.0, 4.0), (5.0, 4.0)]
(4195, 4995),[(2.0, 4.0), (2.0, 4.0)]
(4195, 4975),[(2.0, 4.0), (2.0, 4.0)]
(4975, 4995),[(4.0, 4.0), (4.0, 4.0)]
(4973, 4995),[(4.5, 4.0), (4.5, 4.0)]
"""

def main():

    # Used for testing the mapper function

    import StringIO
    sys.stdin = StringIO.StringIO(test_data)
    reducer()
    sys.stdin = sys.__stdin__

main()
# '''

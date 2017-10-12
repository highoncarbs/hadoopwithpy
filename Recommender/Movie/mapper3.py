#!/usr/bin/env python
# -*- coding: utf-8 -*-
import ast

'''
This mapper sorts similar movies.
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


def mapper():
    '''
    INPUT : movieid , score , count
    OUPUT : movie , recommended movie
    '''
    for line in sys.stdin:
        # Need to check the input
        # Accoring to flow , this needs a moviepair and ist of ratings

        line = ast.literal_eval(line)
        movie_pair , score_rating_pair = line
        try:
            movie1 , movie2 = movie_pair
            score , total_ratings = score_rating_pair
            print("{0} , {1} {2}{3}".format(movie1,movie2, [score],[total_ratings]))
        except:
            pass
# '''
# Testbed

test_data = '''(4973, 4993),(0.9957241920665914, 4)
(4993, 4995),(0.9999999999999998, 2)
(4195, 4995),(0.9999999999999998, 2)
(4195, 4975),(0.9999999999999998, 2)
(4975, 4995),(0.9999999999999998, 2)
(4973, 4995),(1.0, 2)
'''

def main():

    # Used for testing the mapper function

    import StringIO
    sys.stdin = StringIO.StringIO(test_data)
    mapper()
    sys.stdin = sys.__stdin__

main()
# '''

#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This mapper builds combinations of Movie and ratings per user.
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
from itertools import combinations
def mapper():
    # Mapper-2
    for line in sys.stdin:
        # Each line holds ratings and movie id per user
        data = line.strip().split(',' , 1)
        userid , ratings = data
        rating_pair = []
        for rat1 , rat2 in combinations(ratings , 2):
            # combinations will give two tuples/list
            m1 = rat1[0] #Movieid-1
            r1 = rat1[1] #rating-1

            m2 = rat2[0] #Movieid-2
            r2 = rat2[1] #rating-2

            # Now we'll append combination
            # rating_pair.append((m1,m2))
            # rating_pair.append((r1,r2))
            print "{0},{1}".format((m1,m2),(r1,r2))
            # Prints the second combination
            print "{0},{1}".format((m2,m1),(r2,r1))

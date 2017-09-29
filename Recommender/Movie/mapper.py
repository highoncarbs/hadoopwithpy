#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
This mapper gets UderId , movie  and ratings from rating,csv.
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
        From Mapper1 : we need only UserID , (MovieID , rating)
        as output.
    '''

    #* First mapper

    # Read input line
    for line in sys.stdin:
        # Strip whitespace and delimiter - ','
        print line
        data = line.strip().split(',')

        if len(data) == 4:
            # Using array to print out values
            # Direct printing , makes python interpret
            # values with comma in between as tuples
            # tempout = []
            userid , movieid , rating , timestamp = data
            # tempout.append(userid)
            # tempout.append((movieid , float(rating)))
            # print tempout

            #
            print "{0},({1},{2})".format(userid , movieid , rating)

    #
# mapper()

# '''

# TestBed
test_data = """ 671,4973,4.5,1064245471\n
671,4993,5.0,1064245483\n
671,4995,4.0,1064891537\n
671,5010,2.0,1066793004\n
671,5218,2.0,1065111990\n
671,5299,3.0,1065112004\n
671,5349,4.0,1065111863\n
671,5377,4.0,1064245557\n
671,5445,4.5,1064891627\n
671,5464,3.0,1064891549\n
671,5669,4.0,1063502711\n
"""

def main():

	# Used for testing the mapper function

	import StringIO
	sys.stdin = StringIO.StringIO(test_data)
	mapper()
	sys.stdin = sys.__stdin__

main()
# '''

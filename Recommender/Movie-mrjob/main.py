'''
This package uses MrJob to solve the same movie Recommendation
issue , but uses Yelp's MrJob instead of Hadoop Streaming libs.

# Process same as before :
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

from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt

class Recommender(MRJob):

    def configure_options(self):
        super(Recommender() , self).configure_options()
        self.add_file_option('--items' , )

import Pyro4
import pandas as pd
import time

path = "./database/ratings.csv"

ratings = pd.read_csv(path, index_col=["userId", "movieId"])

@Pyro4.expose
class GreetingMaker(object):

    def get_fortunate(self, name):

        return "Hello, {}. Here is your fortune message:\nBehold the warranty --  the bold print giveth and the fine " \
               "print giveth away".format(name)


daemon = Pyro4.Daemon()
uri = daemon.register(GreetingMaker)

# print("Ready. Object uri = ", uri)
# daemon.requestLoop()

# Ah. I'd presumably have to encapsulate this entire thing.

# We're going to need to load in the database.
# Shouldn't that be a simple query, though? Why not just host the back-end in three different places?
# Client interacts with front-end, they send off to "replica-managers"
# RM's periodically exchange gossip messages to cnvey the updates that they have received from their clients.
# The front-ends send queries and updates to any replica manager that they choose.cls

# Maybe I should just java this.

print(ratings)

# Ugh.

def submit_rating(user_id, movie_id, rating):

    # It already gets complicated! I need a nice simple Pythonic SQL

    rating = pd.Series([rating, time.time()], name=user_id)

    ratings.append(rating)

    print(ratings)

    # ratings.to_csv()


def update_rating(user_id, movie_id, rating):

    print(user_id, movie_id, rating)

    # This will basically call the previous one, having removed
    # the specified rating.

def get_ratings(movie_id):

    print(movie_id)

# For now, I suggest that we use pandas. It's pretty easy.


submit_rating("userId", "movieId", "5")


# Create object instances
# Give names to those instances, and register them with the name server
# Tell Pyro to take care of those instances
# Tell Pyro to sit idle in a loop waiting for incoming method calls

# The way I see it: I'm missing the distributor in the middle.

# What's the best way to query?
# We can switch to a mongoDB database later, I think. It rather defeats the purpose otherwise, right?
# Each one would need their own database. Which, in fact, is not crazy.
# Main should create the objects, name then, and assign them to particular names.

# Each server needs to be able to arbitrarily report itself as active, overloaded, of ffline.
# I think it's intended that I run these three, right? Is that possible?

# At least three servers should be implemented"
# Ah. So my front-end acts as this service-broker, essentially. B
# So I have my client program, a front-end server , and then my replications servers
# That's irrelevant for now. Let's just make the program/
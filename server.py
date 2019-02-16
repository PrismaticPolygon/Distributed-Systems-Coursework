import Pyro4
import pandas as pd
import os
import socket
import random
import sys

# Oh, I do that outside. I'm registering the class itself. That logic should therefore be in the front-end.
# Or we have an object-within-an-object. The server handles the bad stuff.

# I'm surprised that there are multiple front-ends shown.
# Two types of operation: query (read-only) and updates (write-only)
# From an implementation perspective? Pass a list of name-servers?
# We'll have to inverse the current relationship, which... shouldn't be impossible.
# With that, they can periodically call methods on each other.
# May as well be after receiving request.s

WORKERNAME = "Worker_%d@%s" % (os.getpid(), socket.gethostname())
statuses = ["ACTIVE", "OVERLOADED", "OFFLINE"]

# Maintain update log
# "Each front end". Why are there multiple? Keeps a vector timestamp that reflects the version of the latest data
# values accessed by the front end (and therefore client).
# FE sends this in every request message to a replica manager, together with query or update.


@Pyro4.expose
class Server(object):

    path = "./database/ratings.csv"
    ratings = pd.read_csv(path)

    def get_rating(self, movie_id):

        return self.ratings.loc[self.ratings["movieId"] == movie_id]["rating"].mean().round()

    def get_status(self):

        return random.choice(statuses)


daemon = Pyro4.Daemon()
uri = daemon.register(Server())

dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")
dispatcher.add_server(uri)

# Okay. Now I can query the dispatcher for server URIs, and then send it directly.
# Or perhaps every server ought to maintain a list... Yeah, I like that. This is not as OO as I'd like.

daemon.requestLoop()

# So how do I get my URI out? I can look it up, if I know my object name

# And these are all in different threads. So there's no problem having it like this.

# def process(item):
#
#     print("Processing: ", item)
#
#     return 0
#
#
# def main():
#
#     dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")
#
#     print("This is worker %s" % WORKERNAME)
#     print("getting work from dispatcher")
#
#     while True:
#
#         try:
#
#             item = dispatcher.getWork()
#
#         except ValueError:
#
#             print("No work available")
#
#         else:
#
#             process(item)
#             dispatcher.putResult(item)
#
#
# if __name__ == "__main__":
#
#     main()

# On creation...

# @Pyro4.expose
# class GreetingMaker(object):
#
#     def get_fortunate(self, name):
#
#         return "Hello, {}. Here is your fortune message:\nBehold the warranty --  the bold print giveth and the fine " \
#                "print giveth away".format(name)
#
#
# daemon = Pyro4.Daemon()
# uri = daemon.register(GreetingMaker)

# print("Ready. Object uri = ", uri)
# daemon.requestLoop()

# Ah. I'd presumably have to encapsulate this entire thing.

# We're going to need to load in the database.
# Shouldn't that be a simple query, though? Why not just host the back-end in three different places?
# Client interacts with front-end, they send off to "replica-managers"
# RM's periodically exchange gossip messages to cnvey the updates that they have received from their clients.
# The front-ends send queries and updates to any replica manager that they choose.cls

# Maybe I should just java this.

# Am I being stupid? Maybe have no index? Yeah.

# Okay! Nice. Now, I can have multiple ones of these.

# Now make this a Pyro object...



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
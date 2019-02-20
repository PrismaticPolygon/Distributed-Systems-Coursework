import Pyro4
from frontend import Method

dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")

# WE'll use CRUD operations for this one.
# Then the front-end is effectively a DNS server, but methods can't be called on it.
# It should be in the dispatcher class. I can specify the names, after all.

# CREATE, READ, UPDATE, DELETE

item = {
    "method": Method.CREATE,
    "rating": {
        "userId": 1,
        "movieId:": 1,
        "rating": 4.0
    }
}

dispatcher.putWork(item)

# This really will be that simple. We'll want to rate-test it, eventually.



# Distinction is not as clear as I'd like by half. On the other hand: I'm just

# So I place work on the dispatcher, I suppose with the standard get/put/requests.
# But how will my gossip architecture work? It requires that the workers know of each other's existences .
# Which means that this simplistic example is unsuitable.

# So should this call a Pyro object?
# Or should it send an HTTP request? It may as well be the former: that's a lot easier.

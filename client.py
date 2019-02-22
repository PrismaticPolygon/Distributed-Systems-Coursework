import Pyro4
# Get a front-end.
import time
import curses
from frontend import Frontend
from request import Request
from enums import Method
import uuid
# from curses import wrapper
#
#
# def main(stdscr):
#
#     # Clear screen
#     stdscr.clear()
#
#     # This raises ZeroDivisionError when i == 10.
#     for i in range(0, 11):
#         v = i - 10
#         stdscr.addstr(i, 0, '10 divided by {} is {}'.format(v, 10 / v))
#
#     stdscr.refresh()
#     stdscr.getkey()


# So how do I want to call? Operations are create, read, and update.

# Because it's in main I guess?
# I'm not doing everything as goddamn dicts!

if __name__ == "__main__":

    print("Getting frontend...")

    ns = Pyro4.locateNS()
    frontends = ns.list(metadata_all={"resource:frontend"})

    if len(frontends) is 0:

        print("No frontends available")

    else:

        name, uri = next(iter(frontends.items()))

        print("Using {0}".format(name), end="\n\n")

        with Pyro4.Proxy(uri) as frontend:

            request = {
                "method": Method.READ,
                "params": {
                    "movie_id": 1
                }
            }

            operation = input("Please enter an operation: (READ): ")

            response = frontend.request(request)

            print(response)



    # if operation == "READ":


    # screen = curses.initscr()
    #
    # curses.noecho()
    # curses.cbreak()
    #
    # screen.keypad(True)
    #
    # screen.clear()
    #
    # time.sleep(3)
    #
    # # Reverse settings above and close process
    # curses.nocbreak()
    # screen.keypad(False)
    # curses.echo()
    # curses.endwin()




# from frontend import Method

# dispatcher = Pyro4.core.Proxy("PYRONAME:example.distributed.dispatcher")

# WE'll use CRUD operations for this one.
# Then the front-end is effectively a DNS server, but methods can't be called on it.
# It should be in the dispatcher class. I can specify the names, after all.

# CREATE, READ, UPDATE, DELETE

# item = {
#     "method": Method.CREATE,
#     "rating": {
#         "userId": 1,
#         "movieId:": 1,
#         "rating": 4.0
#     }
# }




# dispatcher.putWork(item)

# This really will be that simple. We'll want to rate-test it, eventually.



# Distinction is not as clear as I'd like by half. On the other hand: I'm just

# So I place work on the dispatcher, I suppose with the standard get/put/requests.
# But how will my gossip architecture work? It requires that the workers know of each other's existences .
# Which means that this simplistic example is unsuitable.

# So should this call a Pyro object?
# Or should it send an HTTP request? It may as well be the former: that's a lot easier.

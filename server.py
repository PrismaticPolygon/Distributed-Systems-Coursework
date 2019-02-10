import Pyro4

@Pyro4.expose
class GreetingMaker(object):

    def get_fortunate(self, name):

        return "Hello, {}. Here is your fortune message:\nBehold the warranty --  the bold print giveth and the fine " \
               "print giveth away".format(name)


daemon = Pyro4.Daemon()
uri = daemon.register(GreetingMaker)

print("Ready. Object uri = ", uri)
daemon.requestLoop()
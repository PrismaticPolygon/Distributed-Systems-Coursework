import sys
from random import randint

import Pyro4
from Pyro4.errors import NamingError, CommunicationError

from enums import Operation
from requests import ClientRequest


class Client:

    def __init__(self, id: str=None):

        self.id: str = id if id is not None else str(randint(0, 611))
        self.ns = Pyro4.locateNS()

        self.commands = [{
            "text": "Get average rating",
            "operation": Operation.AVERAGE
        }, {
            "text": "Create user rating",
            "operation": Operation.CREATE
        }, {
            "text": "Get user rating",
            "operation": Operation.READ
        }, {
            "text": "Update user rating",
            "operation": Operation.UPDATE
        }, {
            "text": "Delete user rating",
            "operation": Operation.DELETE
        }, {
            "text": "Get all user ratings",
            "operation": Operation.ALL
        }]

    def get_frontend_uri(self) -> Pyro4.URI:
        """
        Get the URI of an available Frontend.
        :return: The URI of a FE
        """

        while True:

            print("\nGetting frontend...")

            try:

                frontends = self.ns.list(metadata_all={"resource:frontend"})

                name, uri = next(iter(frontends.items()))

                print("Using {0}".format(name))

                return uri

            except NamingError as e:

                print("NamingError: ", e)

            except CommunicationError as e:

                print("Communication error", e)

            except StopIteration as e:

                print("No frontends available")

            try_again = input("\nTry again? (Y/N): ").strip()

            if try_again != "Y":

                print("Exiting client")
                sys.exit()

            print("")

    def request(self) -> None:
        """
        Make a request to a FE. Users select a command from a list.
        :return: None
        """

        params = {"user_id": self.id}

        while True:

            for i, command in enumerate(self.commands):

                print(str(i + 1) + ". " + command['text'])

            operation_input = input("\nPlease choose a command (1 - {}): ".format(len(self.commands))).strip()#

            try:

                i = int(operation_input)

                if 1 <= i <= len(self.commands):

                    operation = self.commands[i - 1]['operation']

                    break

                else:

                    raise ValueError

            except ValueError:

                print("Invalid operation entered\n")

        if operation is not Operation.ALL:

            params["movie_id"] = input("Please enter a movie ID: ").strip()

        if operation in [Operation.CREATE, Operation.UPDATE]:

            params["rating"] = input("Please enter a rating: ").strip()

        if operation is Operation.AVERAGE:

            del params['user_id']

        request = ClientRequest(operation, params)
        frontend_uri = self.get_frontend_uri()

        print("\nRequesting {0}...".format(request))

        with Pyro4.Proxy(frontend_uri) as frontend:

            try:

                response = frontend.request(request)

                if request.method in [operation.READ, operation.AVERAGE]:

                    print("Received {0}".format(response), end="\n\n")

                elif request.method is Operation.ALL:

                    print("Received:\n")

                    for rating in response:

                        print(rating)

                    print()

                else:

                    print("Request complete", end="\n\n")

            except ConnectionRefusedError as e:

                print("Error:", e, end="\n\n")

            except CommunicationError as e:

                print("Error:", e, end="\n\n")

    # Hm. It's not even clear what the error is!


if __name__ == "__main__":

    client_id = sys.argv[1] if len(sys.argv) >= 2 else None

    try:

        print("\nCreating client...")

        client = Client(client_id)

        print("client-{} running\n".format(client.id))

        while True:

            client.request()

            try_again = input("Again? (Y/N): ").strip()

            if try_again != "Y":

                break

            print("")

    except KeyboardInterrupt:

        print("Exiting client\n")

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
        Make a request to a frontend
        :return: None
        """

        params = {"user_id": self.id}

        while True:

            operation_input = input("Please enter an operation (CREATE/READ/UPDATE/DELETE): ").strip()

            try:

                operation = Operation(operation_input)
                break

            except ValueError:

                print("Invalid operation entered\n")

        params["movie_id"] = input("Please enter a movie ID: ").strip()

        if operation is not Operation.READ and operation is not Operation.DELETE:

            params["rating"] = input("Please enter a rating: ").strip()

        request = ClientRequest(operation, params)
        frontend_uri = self.get_frontend_uri()

        print("\nRequesting {0}...".format(request))

        with Pyro4.Proxy(frontend_uri) as frontend:

            try:

                response = frontend.request(request)

                if request.method == operation.READ:

                    print("Received {0}".format(response), end="\n\n")

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

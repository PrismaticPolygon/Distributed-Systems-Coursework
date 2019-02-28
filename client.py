import Pyro4
from random import randint

from enums import Operation
from requests import ClientRequest
from frontend import Frontend
from Pyro4.errors import NamingError, CommunicationError
import sys


class Client:

    def __init__(self, id: str=None):

        self.id: str = id if id is not None else str(randint(0, 611))
        self.frontend_uri: Pyro4.URI = self.get_frontend_uri()

    def get_frontend_uri(self) -> Frontend:

        while True:

            print("Getting frontend...")

            try:

                ns = Pyro4.locateNS()
                frontends = ns.list(metadata_all={"resource:frontend"})

                name, uri = next(iter(frontends.items()))

                print("Using {0}".format(name), end="\n\n")

                return uri

            except NamingError as e:

                print(e)

            except CommunicationError as e:

                print(e)

            try_again = input("\nTry again? (Y/N): ").strip()

            if try_again != "Y":

                sys.exit()

            print("")

    def request(self):

        params = {"user_id": self.id}

        while True:

            operation_input = input("Please enter an operation (CREATE/READ/UPDATE/DELETE): ").strip()

            try:

                operation = Operation(operation_input)
                break

            except ValueError:

                print("Invalid operation entered\n")

        params["movie_id"] = input("Please enter a movie ID: ").strip()

        if operation is not Operation.READ:

            params["rating"] = input("Please enter a rating: ").strip()

        request = ClientRequest(operation, params)

        print("\nRequesting {0}...".format(request))

        with Pyro4.Proxy(self.frontend_uri) as frontend:

            try:

                response = frontend.request(request)

                if request.method == operation.READ:

                    print("Received {0}".format(response), end="\n\n")

                else:

                    print("Request complete", end="\n\n")

            except ConnectionRefusedError as e:

                print("Error:", e)


if __name__ == "__main__":

    client = Client("1")

    while True:

        client.request()

        try_again = input("Again? (Y/N): ").strip()

        if try_again != "Y":

            break

        print("")


# Handle ConnectionClosedError

# Handle CommunicationError?
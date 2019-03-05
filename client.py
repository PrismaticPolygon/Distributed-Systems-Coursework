import sys
from random import randint

import Pyro4
from Pyro4.errors import NamingError, CommunicationError

from enums import Operation
from requests import ClientRequest


class Client:

    def __init__(self, id: str=None):

        self.id: str = id if id is not None else str(randint(0, 611))   # The ID of this user
        self.ns = Pyro4.locateNS()  # The Pyro Name Server

        self.commands = [{  # The commands available to the user
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
        Get the URI of an available Frontend. Handle errors and prompt the user to try again on failure.
        :return: The URI of a FE
        """

        while True:

            print("\nGetting frontend...")

            try:

                frontends = self.ns.list(metadata_all={"resource:frontend"})    # Get all registered FEs

                name, uri = next(iter(frontends.items()))   # Get the first one available

                print("Using {0}".format(name))

                return uri

            except NamingError as e:

                print("NamingError: ", e)

            except CommunicationError as e:

                print("Communication error", e)

            except StopIteration as e:

                print("No frontends available")

            try_again = input("\nTry again? (Y/N): ").strip()   # Prompt the user to try again on failure

            if try_again != "Y":

                raise KeyboardInterrupt

            print("")

    def get_operation(self) -> Operation:
        """
        Get an operation from user input. If the user enters an invalid operation, prompt them to try again.
        :return: An Operation that the user wants to perform
        """
        while True:

            for i, command in enumerate(self.commands):     # Print out available operations

                print(str(i + 1) + ". " + command['text'])

            operation_input = input("\nPlease choose a command (1 - {}): ".format(len(self.commands))).strip()

            try:

                i = int(operation_input)    # Cast the input to an int

                if 1 <= i <= len(self.commands):

                    return self.commands[i - 1]['operation']    # Map the input to an operation

                else:

                    raise ValueError    # Raise an error if the user has entered an invalid operation

            except ValueError:

                print("Invalid operation entered\n")

    def build_request(self) -> ClientRequest:
        """
        Build a ClientRequest, adding and removing parameters as necessary.
        :return: A ClientRequest that the user wants to execute
        """

        params = {"user_id": self.id}

        operation = self.get_operation()

        if operation is not Operation.ALL:

            params["movie_id"] = input("Please enter a movie ID: ").strip()

        elif operation in [Operation.CREATE, Operation.UPDATE]:

            params["rating"] = input("Please enter a rating: ").strip()

        elif operation is Operation.AVERAGE:

            del params['user_id']

        return ClientRequest(operation, params)

    def request(self) -> None:
        """
        Make a ClientRequest to a FE. Handle appropriate errors.
        :return: None
        """

        request = self.build_request()  # Build the request
        frontend_uri = self.get_frontend_uri()  # Get an available FE URI

        print("\nRequesting {0}...".format(request))

        with Pyro4.Proxy(frontend_uri) as frontend:

            try:

                response = frontend.request(request)    # Make the request

                print(response)     # Display the response

            except ConnectionRefusedError as e:

                print("Error:", e)

            except CommunicationError as e:

                print("Error:", e)

        print("\n")


if __name__ == "__main__":

    client_id = sys.argv[1] if len(sys.argv) >= 2 else None  # Get the client ID from command-line arguments

    try:

        print("\nCreating client...")

        client = Client(client_id)  # Create a client with the specified ID

        print("client-{} running\n".format(client.id))

        while True:

            client.request()    # Make requests until the user wants to exit

            try_again = input("Again? (Y/N): ").strip()

            if try_again != "Y":

                raise KeyboardInterrupt

            print("")

    except KeyboardInterrupt:

        print("Exiting client\n")
        sys.exit()

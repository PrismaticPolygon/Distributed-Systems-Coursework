import Pyro4
from requests import ClientRequest
from enums import Method

# Okay! What's the next step? Let's get isolated CRUD operations working!

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

            params = {}
            method = None

            while True:

                operation = input("Please enter an operation: (CREATE/READ/UPDATE): ").strip()
                params["movie_id"] = input("Please enter a movie ID: ").strip()

                if operation == "CREATE":

                    method = Method.CREATE
                    params["rating"] = input("Please enter a rating: ").strip()
                    params["user_id"] = 1

                elif operation == "READ":

                    method = Method.READ

                elif operation == "UPDATE":

                    method = Method.UPDATE
                    params["rating_id"] = input("Please enter a rating ID: ").strip()
                    params["user_id"] = 1

                request = ClientRequest(method, params)

                print("Sending request", request)

                response = frontend.request(request)

                print(response)

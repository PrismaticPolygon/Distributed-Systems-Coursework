import sqlite3
import time
from requests import ClientRequest
from enums import Operation


class DB:

    path = "./database/ratings.sql"

    def execute_request(self, request: ClientRequest):

        print("Executing client request", request)

        method: Operation = request.method
        params = request.params
        value = None

        if method == Operation.CREATE:

            self.create(**params)

        elif method == Operation.READ:

            value = self.read(**params)

        elif method == Operation.UPDATE:

            self.update(**params)

        return value

    # No con

    def create(self, user_id, movie_id, rating) -> None:

        print("Creating rating ({0}, {1}, {2})...".format(user_id, movie_id, rating))

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, time.time(),))

        # connection.commit()

        print("Rating created!", end="\n\n")

    def read(self, user_id: str, movie_id: str, ):

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        print("Reading ratings ({0}, {1})...".format(user_id, movie_id))

        cursor.execute("SELECT * FROM ratings WHERE (movieId=? AND userID=?)", (movie_id, user_id))

        # There should only be one, naturally.

        rating = cursor.fetchall()

        print("Calculated", rating, end="...\n")

        return rating

    def update(self, movie_id, user_id, rating) -> None:

        print("Updating rating ({0}, {1}, {2})...".format(user_id, movie_id, rating))

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("UPDATE ratings SET rating=? WHERE (movieID=? AND userId=?)", (rating, movie_id, user_id,))

        # connection.commit()

        print("Rating updated!", end="\n\n")

    def delete(self, movie_id, user_id):

        print("Deleting rating ({0}, {1})...".format(user_id, movie_id,))

        connection = sqlite3.connect(self.path)

        cursor = connection.cursor()

        cursor.execute("DELETE FROM ratings WHERE (movieID=? AND userId=?)", (movie_id, user_id,))

        # connection.commit()

        print("Rating deleted!", end="\n\n")


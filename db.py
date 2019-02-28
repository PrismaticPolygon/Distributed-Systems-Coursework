import sqlite3
import time
from requests import ClientRequest
from enums import Operation


class DB:

    path = "./database/data.sqlite"

    def execute_request(self, request: ClientRequest):

        print("Executing request", request)

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

    def create(self, user_id, movie_id, rating) -> None:

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, time.time(),))

        connection.commit()

        print("Rating created!", end="\n\n")

    def read(self, user_id: str, movie_id: str):

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("SELECT rating FROM ratings WHERE (userId=? AND movieId=?)", (user_id, movie_id))

        result = cursor.fetchone()
        rating = result[0] if result is not None else None

        print("Rating is {0}!".format(rating), end="\n\n")

        return rating

    def update(self, movie_id, user_id, rating) -> None:

        connection = sqlite3.connect(self.path)
        cursor = connection.cursor()

        cursor.execute("UPDATE ratings SET rating=? WHERE (movieId=? AND userId=?)", (rating, movie_id, user_id,))

        connection.commit()

        print("Rating updated!", end="\n\n")

    def delete(self, movie_id, user_id):

        connection = sqlite3.connect(self.path)

        cursor = connection.cursor()

        cursor.execute("DELETE FROM ratings WHERE (movieId=? AND userId=?)", (movie_id, user_id,))

        connection.commit()

        print("Rating deleted!", end="\n\n")


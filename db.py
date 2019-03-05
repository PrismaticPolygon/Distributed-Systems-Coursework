import sqlite3
import time

from enums import Operation
from requests import ClientRequest
from typing import Any


class DB:

    def __init__(self):

        self.path = "./database/ratings.sqlite"

        connection = sqlite3.connect(self.path)    # Load the database on file
        script = "".join(connection.iterdump())  # Convert it into a script

        self.connection = sqlite3.connect(":memory:", check_same_thread=False)  # Connect to an in-memory DB
        self.connection.executescript(script)   # Load the file database into it

    def execute_request(self, request: ClientRequest) -> Any:

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

        elif method == Operation.DELETE:

            self.delete(**params)

        elif method == Operation.AVERAGE:

            value = self.average(**params)

        elif method == Operation.ALL:

            value = self.all(**params)

        return value

    def create(self, user_id, movie_id, rating) -> None:

        cursor = self.connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, time.time(),))

        self.connection.commit()

        print("Rating created!", end="\n\n")

    def read(self, user_id: str, movie_id: str) -> float:

        cursor = self.connection.cursor()

        cursor.execute("SELECT rating FROM ratings WHERE (userId=? AND movieId=?)", (user_id, movie_id))

        result = cursor.fetchone()
        rating = result[0] if result is not None else None

        print("Rating is {0}!".format(rating), end="\n\n")

        return rating

    def update(self, movie_id: str, user_id: str, rating) -> None:

        cursor = self.connection.cursor()

        cursor.execute("UPDATE ratings SET rating=? WHERE (movieId=? AND userId=?)", (rating, movie_id, user_id,))

        self.connection.commit()

        print("Rating updated!", end="\n\n")

    def delete(self, movie_id: str, user_id: str) -> None:

        cursor = self.connection.cursor()

        cursor.execute("DELETE FROM ratings WHERE (movieId=? AND userId=?)", (movie_id, user_id,))

        self.connection.commit()

        print("Rating deleted!", end="\n\n")

    def average(self, movie_id: str) -> Any:

        cursor = self.connection.cursor()

        cursor.execute("SELECT ROUND(AVG(rating)) FROM ratings WHERE (movieId=?)", (movie_id,))

        result = cursor.fetchone()

        rating = result[0] if result is not None else None

        print("Average rating is {0}!".format(rating), end="\n\n")

        return rating

    def all(self, user_id: str) -> Any:

        cursor = self.connection.cursor()

        cursor.execute("SELECT movieId, rating FROM ratings WHERE (userId=?)", (user_id,))

        result = cursor.fetchall()

        print("All ratings:\n")

        for rating in result:

            print(rating)

        return result


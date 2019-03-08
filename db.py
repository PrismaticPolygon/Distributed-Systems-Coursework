import sqlite3
import time
from typing import Any

from enums import Operation
from requests import ClientRequest


class DB:
    def __init__(self):

        self.path = "./database/data.sqlite"

        connection = sqlite3.connect(self.path)  # Load the database on file
        script = "".join(connection.iterdump())  # Convert it into a script

        self.connection = sqlite3.connect(":memory:", check_same_thread=False)  # Connect to an in-memory DB
        self.connection.executescript(script)  # Load the file database into it

    def execute_request(self, request: ClientRequest) -> Any:

        print("Executing request", request)

        method: Operation = request.method
        params = request.params
        value = None

        if method == Operation.CREATE:

            value = self.create(**params)

        elif method == Operation.READ:

            value = self.read(**params)

        elif method == Operation.UPDATE:

            value = self.update(**params)

        elif method == Operation.DELETE:

            value = self.delete(**params)

        elif method == Operation.AVERAGE:

            value = self.average(**params)

        elif method == Operation.ALL:

            value = self.all(**params)

        print(value, end="\n\n")

        return value

    def create(self, user_id, movie_id, rating) -> str:

        cursor = self.connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, time.time(),))

        self.connection.commit()

        return "Rating for " + self.title(movie_id) + " created: " + str(rating)

    def read(self, user_id: str, movie_id: str) -> str:

        cursor = self.connection.cursor()

        cursor.execute("SELECT rating FROM ratings WHERE (userId=? AND movieId=?)", (user_id, movie_id))

        result = cursor.fetchone()
        rating = result[0] if result is not None else None

        return "Rating for " + self.title(movie_id) + ": " + str(rating)

    def update(self, movie_id: str, user_id: str, rating) -> str:

        cursor = self.connection.cursor()

        cursor.execute("UPDATE ratings SET rating=?, timestamp=? WHERE (movieId=? AND userId=?)", (rating, time.time(),
                                                                                                   movie_id, user_id,))

        self.connection.commit()

        return "Rating for " + self.title(movie_id) + " updated: " + str(rating)

    def delete(self, movie_id: str, user_id: str) -> str:

        cursor = self.connection.cursor()

        cursor.execute("DELETE FROM ratings WHERE (movieId=? AND userId=?)", (movie_id, user_id,))

        self.connection.commit()

        return "Rating for " + self.title(movie_id) + " deleted"

    def average(self, movie_id: str) -> str:

        cursor = self.connection.cursor()

        cursor.execute("SELECT ROUND(AVG(rating)) FROM ratings WHERE (movieId=?)", (movie_id,))

        result = cursor.fetchone()

        rating = result[0] if result is not None else None

        return "Average rating for " + self.title(movie_id) + ": " + str(rating)

    def title(self, movie_id: str) -> str:

        cursor = self.connection.cursor()

        cursor.execute("SELECT title FROM movies WHERE (movieId=?)", (movie_id,))

        result = cursor.fetchone()

        return result[0] + " (" + movie_id + ")"

    def all(self, user_id: str) -> Any:

        cursor = self.connection.cursor()

        cursor.execute("SELECT movieId, rating FROM ratings WHERE (userId=?)", (user_id,))

        result = cursor.fetchall()

        return [self.title(rating[0]) + ": " + str(rating[1]) for rating in result]

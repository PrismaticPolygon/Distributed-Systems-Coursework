import sqlite3
import time


class DB:

    def create(self, user_id, movie_id, rating) -> None:

        print("Creating rating ({0}, {1}, {2})...".format(user_id, movie_id, rating))

        connection = sqlite3.connect("./database/ratings.db")

        cursor = connection.cursor()

        cursor.execute("INSERT INTO ratings VALUES (?, ?, ?, ?)", (user_id, movie_id, rating, time.time(),))

        connection.commit()

        print("Rating created!", end="\n\n")

    def read(self, movie_id: str) -> float:

        connection = sqlite3.connect("./database/ratings.db")

        cursor = connection.cursor()

        print("Reading rating for {0}...".format(movie_id))

        cursor.execute("SELECT ROUND(AVG(rating)) FROM ratings WHERE movieId=?", (movie_id,))

        rating = cursor.fetchone()[0]

        print("Calculated", rating, end="...\n")

        return rating

    def update(self, movie_id, user_id, rating) -> None:

        print("Updating rating ({0}, {1}, {2})...".format(user_id, movie_id, rating))

        connection = sqlite3.connect("./database/ratings.db")

        cursor = connection.cursor()

        cursor.execute("UPDATE ratings SET rating=? WHERE (movieID=? AND userId=?)", (rating, movie_id, user_id,))

        connection.commit()

        print("Rating updated!", end="\n\n")

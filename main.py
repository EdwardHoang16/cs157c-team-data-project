from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime


class MovieInfoGuide:
    def __init__(self, client):
        self.client = client
        self.db = client.project

        loop_continues = True
        while loop_continues:
            self.print_options()
            loop_continues = self.choose_option()
            print()

    def print_options(self):
        print("Enter the number corresponding to each function:")
        print("-1 - Quit")
        print("1 - Find movies based on title")
        print("2 - Find movies based on genre")
        print("3 - Add a movie")
        print("4 - Update a movie's details")
        print("5 - Delete a movie")
        print("6 - Find all ratings associated with a movie")
        print("7 - Find all ratings a user has created")
        print("8 - Add a rating for a movie from a user")
        print("9 - Update a user's rating for a movie")
        print("10 - Delete a user's rating for a movie")
        print("11 - Find all movie tags")
        print("12 - Find the most relevant tags for a movie")
        print("13 - Find all tag associations a user has made")
        print("14 - Add a tag association to a movie from a user")
        print("15 - Delete a user's tag association to a movie")

    def choose_option(self):
        option = input("Enter your option: ")
        if option == "1":
            self.get_movies_on_title()
        elif option == "2":
            self.get_movies_on_genre()
        elif option == "3":
            self.add_movie()
        elif option == "4":
            self.update_movie()
        elif option == "5":
            self.delete_movie()
        elif option == "6":
            self.get_movie_ratings()
        elif option == "7":
            self.get_user_ratings()
        elif option == "8":
            self.add_rating()
        elif option == "9":
            self.update_rating()
        elif option == "10":
            self.delete_rating()
        elif option == "11":
            self.get_all_tags()
        elif option == "12":
            self.get_movie_tags()
        elif option == "13":
            self.get_user_created_tags()
        elif option == "14":
            self.add_tag_association()
        elif option == "15":
            self.delete_tag_association()
        elif option == "-1":
            return False
        else:
            print("Invalid input")
        return True

    def get_movies_on_title(self):
        title = input("Enter title: ")

        collection = self.db.movies
        documents = collection.find({"title": {"$regex": title}}, {"_id": 0})
        for document in documents:
            print(document)

    def get_movies_on_genre(self):
        genre = input("Enter genre: ")

        collection = self.db.movies
        documents = collection.find({"genres": {"$regex": genre}}, {"_id": 0})
        for document in documents:
            print(document)

    def add_movie(self):
        movie_id = int(input("Enter movie ID: "))
        title = input("Enter title: ")
        genres = input("Enter genres: ")

        collection = self.db.movies
        new_movie = {"movieId": movie_id, "title": title, "genres": genres}
        collection.insert_one(new_movie)
        document = collection.find_one({"movieId": movie_id})
        print(document)

    def update_movie(self):
        movie_id = int(input("Enter movie ID: "))
        field_to_update = input("Enter field being updated: ")
        new_value = input("Enter new value: ")

        collection = self.db.movies
        filter = {"movieId": movie_id}
        collection.update_one(filter, {"$set": {field_to_update: new_value}})
        document = collection.find_one(filter)
        print(document)

    def delete_movie(self):
        movie_id = int(input("Enter movie ID: "))

        collection = self.db.movies
        collection.delete_one({"movieId": movie_id})

    def get_movie_ratings(self):
        movie_id = int(input("Enter movie ID: "))

        collection = self.db.ratings
        documents = collection.find({"movieId": movie_id}, {"_id": 0})
        for document in documents:
            print(document)

    def get_user_ratings(self):
        user_id = int(input("Enter user ID: "))

        collection = self.db.ratings
        documents = collection.find({"userId": user_id})
        movie_id_ratings = {}
        for document in documents:
            movie_id_ratings[document["movieId"]] = document["rating"]

        collection = self.db.movies
        documents = collection.find()
        for document in documents:
            if document["movieId"] in movie_id_ratings:
                print("{'movieId': " + str(document["movieId"]) + ", 'title': " + str(document["title"])
                      + ", 'rating': " + str(movie_id_ratings[document["movieId"]]) + "}")

    def add_rating(self):
        user_id = int(input("Enter user ID: "))
        movie_id = int(input("Enter movie ID: "))
        rating = float(input("Enter rating: "))

        collection = self.db.ratings
        timestamp = int(datetime.timestamp(datetime.now()))
        collection.insert_one({"userId": user_id, "movieId": movie_id, "rating": rating,
                               "timestamp": timestamp})
        document = collection.find_one({"userId": user_id, "movieId": movie_id, "rating": rating,
                                        "timestamp": timestamp}, {"_id": 0})
        print(document)

    def update_rating(self):
        user_id = int(input("Enter user ID: "))
        movie_id = int(input("Enter movie ID: "))
        rating = float(input("Enter rating: "))

        collection = self.db.ratings
        timestamp = int(datetime.timestamp(datetime.now()))
        filter = {"userId": user_id, "movieId": movie_id}
        new_values = {"$set": {"rating": rating, "timestamp": timestamp}}
        collection.update_one(filter, new_values)
        document = collection.find_one({"userId": user_id, "movieId": movie_id, "rating": rating,
                                        "timestamp": timestamp}, {"_id": 0})
        print(document)

    def delete_rating(self):
        user_id = int(input("Enter user ID: "))
        movie_id = int(input("Enter movie ID: "))

        collection = self.db.ratings
        document = collection.find_one({"userId": user_id, "movieId": movie_id})
        collection.delete_one({"_id": document["_id"]})

    def get_all_tags(self):
        collection = self.db.genomeTags
        documents = collection.find({}, {"_id": 0})
        for document in documents:
            print(document)

    def get_movie_tags(self):
        movie_id = int(input("Enter movie ID: "))

        collection = self.db.genomeScores
        documents = collection.find({"movieId": movie_id})
        tag_id_relevance_scores = {}
        for document in documents:
            tag_id_relevance_scores[document["tagId"]] = document["relevance"]

        collection = self.db.genomeTags
        documents = collection.find()
        tag_name_relevance_scores = {}
        for document in documents:
            tag_name_relevance_scores[document["tag"]] = tag_id_relevance_scores[document["tagId"]]

        tag_name_relevance_scores = dict(
            sorted(tag_name_relevance_scores.items(), key=lambda item: item[1], reverse=True))
        for tag_name in tag_name_relevance_scores:
            print("{'tag': " + str(tag_name) + ", 'relevance': " + str(tag_name_relevance_scores[tag_name]) + "}")

    def get_user_created_tags(self):
        user_id = int(input("Enter user ID: "))

        collection = self.db.tags
        documents = collection.find({"userId": user_id})
        movie_id_to_tag_info = defaultdict(list)
        for document in documents:
            movie_id_to_tag_info[document["movieId"]].append((document["tag"], document["timestamp"]))

        collection = self.db.movies
        documents = collection.find()
        for document in documents:
            if document["movieId"] in movie_id_to_tag_info:
                movie_id = document["movieId"]
                for tag_data in movie_id_to_tag_info[movie_id]:
                    print("{'movieId': " + str(movie_id) + ", 'title': " + str(document["title"])
                          + ", 'tag': " + tag_data[0] + ", 'timestamp': "
                          + str(tag_data[1]) + "}")

    def add_tag_association(self):
        user_id = int(input("Enter user ID: "))
        movie_id = int(input("Enter movie ID: "))
        tag = input("Enter tag: ")

        collection = self.db.tags
        timestamp = int(datetime.timestamp(datetime.now()))
        collection.insert_one({"userId": user_id, "movieId": movie_id, "tag": tag, "timestamp": timestamp})
        document = collection.find_one({"userId": user_id, "movieId": movie_id, "tag": tag,
                                        "timestamp": timestamp}, {"_id": 0})
        print(document)

    def delete_tag_association(self):
        user_id = int(input("Enter user ID: "))
        movie_id = int(input("Enter movie ID: "))
        tag = input("Enter tag: ")

        collection = self.db.tags
        document = collection.find_one({"userId": user_id, "movieId": movie_id, "tag": tag})
        collection.delete_one({"_id": document["_id"]})


if __name__ == '__main__':
    uri = "mongodb://50.17.201.202:27021"
    client = MongoClient(uri)
    movie_info_guide = MovieInfoGuide(client)
    client.close()

import statistics

import pymongo
import pandas as pnd
from config_data import *


class Mongo:
    def __init__(self):
        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.database = self.client["library"]
        self.collection = self.database["books"]
        self.columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

    def create_mongo(self):
        data_iter = pnd.read_csv(CSV_URL, usecols=self.columns_to_tables, iterator=True)

        for data_chunk in data_iter:
            # drop rows with any NA values
            data = data_chunk.dropna()
            data["Title"] = data["Title"].apply(lambda row: row.rsplit(';')[0])

            self.collection.insert_many(data.to_dict('records'), ordered=False, bypass_document_validation=True)

        # Confirm the successful insertion
        print("Book inserted successfully!")

    def close_mongo(self):
        # Close the MongoDB connection
        self.client.close()

    def insert(self, n):
        book_data = {
            'Title': 'Przykładowa książka',
            'Author': 'John Doe',
            'PublicationYear': 2022,
            'Publisher': 'Example Publisher',
            'ItemType': 'Book',
            'ItemCollection': 'Main Collection'
        }

        i = 1
        for _ in range(n):
            new_book_data = {
                'Title': '',
                'Author': 'London, Julia',
                'PublicationYear': 2022,
                'Publisher': 'Example Publisher',
                'ItemType': 'Book',
                'ItemCollection': 'Main Collection'
            }
            book_data2 = book_data['Title'] + str(i)
            new_book_data['Title'] = book_data2
            self.collection.insert_one(new_book_data)
            i = i + 1

    def update(self, publication_year):
        query = {"PublicationYear": {"$lt": publication_year}}
        documents = self.collection.find(query)
        print(documents)

        for doc in documents:
            title = doc["Title"]
            year = doc["PublicationYear"]
            new_title = f"{title} ({year})"

            self.collection.update_one({"_id": doc["_id"]}, {"$set": {"Title": new_title}})

    def select(self, author_name):
        query = {'Author': author_name}
        result = self.collection.find(query)
        documents = [doc for doc in result]

        return documents

    def select_all(self):
        result = self.collection.find()
        documents = [doc for doc in result]

        return documents

    def delete(self, publication_year):
        query = {'PublicationYear': {'$lt': publication_year}}
        self.collection.delete_many(query)

    def select_authors(self):
        authors = self.collection.distinct('Author')

        return authors

    def count_books_by_publisher(self):
        pipeline = [
            {"$group": {"_id": "$Publisher", "count": {"$sum": 1}}}
        ]
        result = self.collection.aggregate(pipeline)
        counts = {doc["_id"]: doc["count"] for doc in result}
        return counts

    def count_words_in_titles(self):
        search_word = "for"
        pipeline = [
            {"$match": {"Title": {"$regex": search_word}}},
            {"$project": {"_id": 0,
                          "count": {"$regexMatch": {"input": "$Title", "regex": search_word, "options": "i"}}}},
            {"$group": {"_id": None, "total_count": {"$sum": {"$cond": [{"$eq": ["$count", True]}, 1, 0]}}}}
        ]
        result = list(self.collection.aggregate(pipeline))
        if result:
            count = result[0]['total_count']
        else:
            count = 0

        return count

    def count_avg_publisher_books(self):
        pipeline = [
            {"$group": {"_id": "$Publisher", "count": {"$sum": 1}}},
            {'$group': {'_id': None, 'averageBooks': {'$avg': '$count'}}}
        ]

        result = self.collection.aggregate(pipeline)
        average_books = next(result)['averageBooks']

        return average_books

    def count_median_for_books_by_publisher(self):
        pipeline = [
            {"$group": {"_id": "$Publisher", "count": {"$sum": 1}}},
            {"$sort": {"count": 1}},
            {"$group": {"_id": None, "counts": {"$push": "$count"}}},
            {"$project": {
                "median": {"$avg": {"$slice": ["$counts", {"$trunc": {"$divide": [{"$size": "$counts"}, 2]}}]}}}}
        ]

        result = self.collection.aggregate(pipeline)
        median = next(result, {}).get("median")

        return median





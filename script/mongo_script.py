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

    def select(self, author_name):
        query = {'Author': author_name}
        result = self.collection.find(query)
        titles = [doc['Title'] for doc in result]

        return titles

    def select_all(self):
        # TODO
        pass

    def delete(self, publication_year):
        query = {'PublicationYear': {'$lt': publication_year}}
        result = self.collection.delete_many(query)

    def update(self, publication_year):
        query = {"PublicationYear": {"$lt": publication_year}}
        documents = self.collection.find(query)
        print(documents)

        for doc in documents:
            title = doc["Title"]
            year = doc["PublicationYear"]
            new_title = f"{title} ({year})"

            self.collection.update_one({"_id": doc["_id"]}, {"$set": {"Title": new_title}})

    def insert(self):
        book_data = {
            'Title': 'Przykładowa książka',
            'Author': 'John Doe',
            'PublicationYear': 2022,
            'Publisher': 'Example Publisher',
            'ItemType': 'Book',
            'ItemCollection': 'Main Collection'
        }

        # Wykonaj 1000 operacji wstawiania w MongoDB
        i = 1
        for _ in range(1000):
            new_book_data = {
                'Title': '',
                'Author': 'John Doe',
                'PublicationYear': 2022,
                'Publisher': 'Example Publisher',
                'ItemType': 'Book',
                'ItemCollection': 'Main Collection'
            }
            book_data2 = book_data['Title'] + str(i)
            new_book_data['Title'] = book_data2
            self.collection.insert_one(new_book_data)
            i = i + 1


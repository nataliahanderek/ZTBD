import pymongo
import pandas as pnd
import sys
import pathlib


class Mongo:
    def __init__(self):
        sys.path.append(str(pathlib.Path.cwd()))
        # print(pathlib.Path.cwd())

        self.client = pymongo.MongoClient("mongodb://localhost:27017/")
        self.database = self.client["library"]
        self.collection = self.database["books"]
        self.columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

    def create_mongo(self):
        data_iter = pnd.read_csv('../data/library-collection-inventory.csv', usecols=self.columns_to_tables,
                                 iterator=True,
                                 chunksize=10000)

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

    def select_mongo(self, author_name):
        query = {'Author': author_name}
        result = self.collection.find(query)
        titles = [doc['Title'] for doc in result]

        return titles

    def delete_mongo(self, publication_year):
        query = {'PublicationYear': {'$lt': publication_year}}
        result = self.collection.delete_many(query)

    def update_mongo(self, publication_year):
        query = {"PublicationYear": {"$lt": publication_year}}
        documents = self.collection.find(query)

        for doc in documents:
            title = doc["Title"]
            year = doc["PublicationYear"]
            new_title = f"{title} ({year})"

            self.collection.update_one({"_id": doc["_id"]}, {"$set": {"Title": new_title}})

    def insert_mongo(self):
        book_data = {
            'Title': 'Przykładowa książka',
            'Author': 'John Doe',
            'PublicationYear': 2022,
            'Publisher': 'Example Publisher',
            'ItemType': 'Book',
            'ItemCollection': 'Main Collection'
        }

        # Wykonaj 1000 operacji wstawiania w MongoDB
        for _ in range(1000):
            self.collection.insert_one(book_data)
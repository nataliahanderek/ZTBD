import pymongo
import pandas as pnd
import sys

import pathlib

sys.path.append(str(pathlib.Path.cwd()))
print(pathlib.Path.cwd())

# Establish a connection to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")
database = client["library"]
collection = database["books"]

columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

data_iter = pnd.read_csv('../data/library-collection-inventory.csv', usecols=columns_to_tables, iterator=True, chunksize=10000)

for data_chunk in data_iter:
    #drop rows with any NA values
    data = data_chunk.dropna()
    data["Title"] = data["Title"].apply(lambda row: row.rsplit(';')[0])

    collection.insert_many(data.to_dict('records'), ordered=False, bypass_document_validation=True)

# Confirm the successful insertion
print("Book inserted successfully!")

# Close the MongoDB connection
client.close()
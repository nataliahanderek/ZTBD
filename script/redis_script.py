import redis
import csv

columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

def create_redis():
    r = redis.Redis(host='localhost', port=6379, db=0)

    with open('../data/library-collection-inventory.csv', 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)

        for i, row in enumerate(csvreader, 1):
            id = str(i)
            title = row['Title']
            author = row['Author']
            year = row['PublicationYear']
            publisher = row['Publisher']
            item_type = row['ItemType']
            item_collection = row['ItemCollection']

            # Tworzenie słownika z danymi
            data = {
                'Title': title,
                'Author': author,
                'PublicationYear': year,
                'Publisher': publisher,
                'ItemType': item_type,
                'ItemCollection': item_collection
            }

            # Dodawanie danych do bazy Redis
            r.hset(id, mapping = data)

    print("Dane z pliku CSV zostały dodane do bazy Redis.")


def select_redis():

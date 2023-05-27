import redis
import csv


class Redis:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

    def create_redis(self):
        with open('C:/Users/hande/OneDrive/Pulpit/ZTBD/data/library-collection-inventory.csv', 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)

            for i, row in enumerate(csvreader, 1):
                book_id = f'book:{i}'

                title = row['Title']
                author = row['Author']
                year = row['PublicationYear']
                publisher = row['Publisher']
                item_type = row['ItemType']
                item_collection = row['ItemCollection']

                # Tworzenie słownika z danymibu
                data = {
                    'Title': title,
                    'Author': author,
                    'PublicationYear': year,
                    'Publisher': publisher,
                    'ItemType': item_type,
                    'ItemCollection': item_collection
                }

                # Dodawanie danych do bazy Redis
                self.r.hset(book_id, mapping=data)

        print("Dane z pliku CSV zostały dodane do bazy Redis.")

    def close_redis(self):
        # Wyłącz połączenie z bazą danych Redis
        self.r.close()

    # works
    def select_redis(self, author_name):
        keys = self.r.keys()
        titles = []

        for key in keys:
            data = self.r.hgetall(key)
            if data[b'Author'] == author_name.encode():
                titles.append(data[b'Title'].decode())

        return titles

    # works
    def select_all_redis(self):
        keys = self.r.keys()
        titles = []

        for key in keys:
            data = self.r.hgetall(key)
            titles.append(data[b'Title'])

        return titles

    def flush(self):
        self.r.flushdb()

    def delete_redis(self, publication_year):
        keys = self.r.keys()
        for key in keys:
            data = self.r.hgetall(key)
            if data[b'PublicationYear'].decode() == publication_year:
                self.r.delete(key)

    # works
    def update_redis(self, publication_year):
        keys = self.r.keys()

        for key in keys:
            data = self.r.hgetall(key)
            title = data.get(b'Title').decode('utf-8')
            year = data.get(b'PublicationYear').decode()

            if year == publication_year:
                new_title = f"{title} ({year})"
                self.r.hset(key, 'Title', new_title)

    def insert_redis(self):
        book_data = {
            'Title': 'Przykładowa książka',
            'Author': 'John Doe',
            'PublicationYear': '2022',
            'Publisher': 'Example Publisher',
            'ItemType': 'Book',
            'ItemCollection': 'Main Collection'
        }

        for _ in range(1000):
            self.r.incr('book:count')  # Zwiększ wartość licznika dla książek
            book_id = self.r.get('book:count')  # Pobierz aktualną wartość licznika jako ID książki
            key = f'book:{book_id.decode()}'
            self.r.hset(key, mapping = book_data)

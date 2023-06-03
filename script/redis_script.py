import statistics
import uuid
import redis
import csv
from config_data import *


class Redis:
    def __init__(self):
        self.r = redis.Redis(host=HOST, port=6379, db=0)
        self.columns_to_tables = ["Title", "Author", "PublicationYear", "Publisher", "ItemType", "ItemCollection"]

    def create_redis(self):
        with open(CSV_URL, 'r', encoding='utf-8') as csvfile:
            csvreader = csv.DictReader(csvfile)

            for i, row in enumerate(csvreader, 1):
                book_id = f'book:{i}'
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
                self.r.hset(book_id, mapping=data)

        print("Dane z pliku CSV zostały dodane do bazy Redis.")

    def close_redis(self):
        # Wyłącz połączenie z bazą danych Redis
        self.r.close()

    def clear(self):
        self.r.flushdb()

    def insert(self, n):
        book_data = {
            'Title': 'Przykładowa książka',
            'Author': 'London, Julia',
            'PublicationYear': '2022',
            'Publisher': 'Example Publisher',
            'ItemType': 'Book',
            'ItemCollection': 'Main Collection'
        }

        for _ in range(n):
            book_id = str(uuid.uuid4())
            key = f'book:{book_id}'
            self.r.hset(key, mapping=book_data)

    def update(self, publication_year):
        keys = self.r.keys()

        for key in keys:
            data = self.r.hgetall(key)
            title = data.get(b'Title').decode('utf-8')
            year = data.get(b'PublicationYear').decode()

            if year == publication_year:
                new_title = f"{title} ({year})"
                self.r.hset(key, 'Title', new_title)

    def select(self, author_name):
        keys = self.r.keys()
        results = []

        for key in keys:
            data = self.r.hgetall(key)
            if data[b'Author'] == author_name.encode():
                result = {k.decode(): v.decode() for k, v in data.items()}
                results.append(result)

        return results

    def select_all(self):
        keys = self.r.keys()
        results = []

        for key in keys:
            data = self.r.hgetall(key)
            result = {k.decode(): v.decode() for k, v in data.items()}
            results.append(result)

        return results

    def delete(self, publication_year):
        keys = self.r.keys()
        for key in keys:
            data = self.r.hgetall(key)
            if data[b'PublicationYear'].decode() == publication_year:
                self.r.delete(key)

    def select_authors(self):
        keys = self.r.keys()
        authors = set()

        for key in keys:
            data = self.r.hgetall(key)
            author = data[b'Author'].decode()
            authors.add(author)

        return list(authors)

    def count_books_by_publisher_redis(self):
        keys = self.r.keys()
        counts = {}

        for key in keys:
            data = self.r.hgetall(key)
            publisher = data[b'Publisher'].decode()

            if publisher in counts:
                counts[publisher] += 1
            else:
                counts[publisher] = 1

        return counts

    def count_words_in_titles(self):
        keys = self.r.keys()
        count = 0
        search_word = 'for'

        for key in keys:
            data = self.r.hgetall(key)
            if b'Title' in data and search_word in data[b'Title'].decode():
                count += data[b'Title'].decode().count(search_word)

        return count

    def count_avg_publisher_books(self):
        keys = self.r.keys()
        publishers = set()

        for key in keys:
            data = self.r.hgetall(key)
            publisher = data[b'Publisher'].decode()
            publishers.add(publisher)

        publishers = len(publishers)
        total_books = self.r.dbsize()

        if publishers > 0:
            average = total_books / publishers
        else:
            average = 0

        return average

    def count_median_for_books_by_publisher(self):
        keys = self.r.keys()
        counts = {}

        for key in keys:
            data = self.r.hgetall(key)
            publisher = data[b'Publisher'].decode()

            if publisher in counts:
                counts[publisher] += 1
            else:
                counts[publisher] = 1

        sorted_counts = sorted(counts.items(), key=lambda x: x[1])
        books_counts = [count for publisher, count in sorted_counts]
        median = statistics.median(books_counts)

        return median


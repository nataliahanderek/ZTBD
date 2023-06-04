import tkinter as tk
from tkinter import ttk, messagebox
import matplotlib.pyplot as plt
import numpy as np

from main import count_time
from script.mongo_script import Mongo
from script.redis_script import Redis
from script.sql_script import MySql


class Gui:
    def __init__(self):
        # launch clients

        self.time_update_mongo = None
        self.mongo_client = Mongo()
        self.redis_client = Redis()
        self.sql_client = MySql()

        self.time_mongo = None
        self.time_redis = None
        self.time_sql = None

        # set times
        self.time_update_mongo = None
        self.time_update_redis = None
        self.time_update_sql = None

        self.time_delete_mongo = None
        self.time_delete_redis = None
        self.time_delete_sql = None

        self.time_insert_mongo = None
        self.time_insert_redis = None
        self.time_insert_sql = None

        self.time_select_mongo = None
        self.time_select_redis = None
        self.time_select_sql = None

        # create GUI elements
        self.root = tk.Tk()
        self.root.title("Configuration")
        self.root.geometry("500x950")

        # Tworzenie stylu dla przycisków
        style = ttk.Style()
        style.configure("CustomButton.TButton",
                        background="blue",
                        foreground="black",
                        font=("Arial", 8))

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n")
        self.spacer.pack()

        # insert Label and Input
        self.label_insert = tk.Label(self.root, text="Podaj ile rekordów chcesz dodać:", width=50)
        self.label_insert.pack()
        self.entry_insert = tk.Entry(self.root, width=50)
        self.entry_insert.pack()

        # create insert button
        self.insert_button = ttk.Button(self.root,
                                        text="Insert",
                                        style="CustomButton.TButton",
                                        command=lambda: self.insert_strategy(int(self.entry_insert.get())))
        self.insert_button.configure(width=50)
        self.insert_button.pack()

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n")
        self.spacer.pack()

        # delete Label and Input
        self.label_delete = tk.Label(self.root, text="Podaj z jakiego roku chcesz usunąć rekordy:", width=50)
        self.label_delete.pack()
        self.entry_delete = tk.Entry(self.root, width=50)
        self.entry_delete.pack()

        # create delete button
        self.delete_button = ttk.Button(self.root,
                                        text="Delete",
                                        width=50,
                                        style="CustomButton.TButton",
                                        command=lambda: self.delete_strategy(int(self.entry_delete.get())))
        self.delete_button.configure(width=50)
        self.delete_button.pack()

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n")
        self.spacer.pack()

        # select Label and Input
        self.label_select = tk.Label(self.root, text="Podaj jakiego autora książki szukasz:")
        self.label_select.pack()
        self.entry_select = tk.Entry(self.root, width=50)
        self.entry_select.pack()

        # create select button
        self.select_button = ttk.Button(self.root,
                                        text="Select",
                                        width=50,
                                        style="CustomButton.TButton",
                                        command=lambda: self.select_strategy(self.entry_select.get()))
        self.select_button.configure(width=50)
        self.select_button.pack()

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n")
        self.spacer.pack()

        # update Label and Input
        self.label_update = tk.Label(self.root, text="Podaj dla jakiego roku chcesz zrobić update:")
        self.label_update.pack()
        self.entry_update = tk.Entry(self.root, width=50)
        self.entry_update.pack()

        # create update button
        self.update_button = ttk.Button(self.root,
                                        text="Update",
                                        style="CustomButton.TButton",
                                        command=lambda: self.update_strategy(int(self.entry_update.get())))
        self.update_button.configure(width=50)
        self.update_button.pack()

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n\n")
        self.spacer.pack()

        self.style = ttk.Style()
        self.style.configure("Custom.Treeview", font=("Arial", 8))

        # Tworzenie tablicy
        self.table = ttk.Treeview(self.root, columns=("Time"), style="Custom.Treeview")
        self.table.heading("#0", text="Database")
        self.table.heading("Time", text="Time (ms)")
        self.table.column("Time", width=150)
        self.table.column("#0", width=150)
        self.table.configure(height=3)
        self.table.tag_configure("oddrow", background="#E8E8E8")
        self.table.tag_configure("evenrow", background="white")
        self.table.pack()

        # Spacer
        self.spacer = ttk.Label(self.root, text="\n")
        self.spacer.pack()

        # create update button
        self.generate_stats_button = ttk.Button(self.root,
                                                text="Generate statistics",
                                                style="CustomButton.TButton",
                                                command=lambda: self.generate_stats())
        self.generate_stats_button.configure(width=50)
        self.generate_stats_button.pack()

    def update_strategy(self, year):
        # modyfikujemy wszystkie rekordy które były w roku: data (String)
        self.time_update_mongo = count_time(lambda: self.mongo_client.update(year))
        self.time_update_redis = count_time(lambda: self.redis_client.update(year))
        self.time_update_sql = count_time(lambda: self.sql_client.update(year))

        self.create_table(self.time_update_mongo, self.time_update_redis, self.time_update_sql)

    def delete_strategy(self, year):
        # usuwamy wszystkie rekordy które były w roku: data (String)
        self.time_delete_mongo = count_time(lambda: self.mongo_client.delete(year))
        self.time_delete_redis = count_time(lambda: self.redis_client.delete(year))
        self.time_delete_sql = count_time(lambda: self.sql_client.delete(year))

        self.create_table(self.time_delete_mongo, self.time_delete_redis, self.time_delete_sql)

    def insert_strategy(self, n):
        # dodajemy przykładowych n rekordów do baz
        self.time_insert_mongo = count_time(lambda: self.mongo_client.insert(n))
        self.time_insert_redis = count_time(lambda: self.redis_client.insert(n))
        self.time_insert_sql = count_time(lambda: self.sql_client.insert(n))

        self.create_table(self.time_insert_mongo, self.time_insert_redis, self.time_insert_sql)

    def select_strategy(self, author):
        # szykamy książek napisanych przez autorów: author
        self.time_select_mongo = count_time(lambda: self.mongo_client.select(author))
        self.time_select_redis = count_time(lambda: self.redis_client.select(author))
        self.time_select_sql = count_time(lambda: self.sql_client.select(author))

        self.create_table(self.time_select_mongo, self.time_select_redis, self.time_select_sql)

    def clean_gui(self):
        self.redis_client.close_redis()
        self.mongo_client.close_mongo()
        self.sql_client.close_sql()
        print('Clients was closed!')

    def create_table(self, time_mongo, time_redis, time_sql):
        self.table.delete(*self.table.get_children())

        self.table.insert("", "end", text="MongoDB", values=(time_mongo), tags=("oddrow",))
        self.table.insert("", "end", text="Redis", values=(time_redis), tags=("evenrow",))
        self.table.insert("", "end", text="SQL", values=(time_sql), tags=("oddrow",))

    def get_times_for_stats(self):
        times = []
        times_mongo = [count_time(lambda: self.mongo_client.select_all()),
                       count_time(lambda: self.mongo_client.select_authors()),
                       count_time(lambda: self.mongo_client.count_books_by_publisher()),
                       count_time(lambda: self.mongo_client.count_words_in_titles()),
                       count_time(lambda: self.mongo_client.count_avg_publisher_books()),
                       count_time(lambda: self.mongo_client.count_median_for_books_by_publisher())]

        times.append(times_mongo)
        print(times_mongo)

        times_redis = [count_time(lambda: self.redis_client.select_all()),
                       count_time(lambda: self.redis_client.select_authors()),
                       count_time(lambda: self.redis_client.count_books_by_publisher_redis()),
                       count_time(lambda: self.redis_client.count_words_in_titles()),
                       count_time(lambda: self.redis_client.count_avg_publisher_books()),
                       count_time(lambda: self.redis_client.count_median_for_books_by_publisher())]

        times.append(times_redis)
        print(times_redis)

        times_mysql = [count_time(lambda: self.sql_client.select_all()),
                       count_time(lambda: self.sql_client.select_authors()),
                       count_time(lambda: self.sql_client.count_books_by_publisher()),
                       count_time(lambda: self.sql_client.count_words_in_titles()),
                       count_time(lambda: self.sql_client.count_avg_publisher_books()),
                       count_time(lambda: self.sql_client.count_median_for_books_by_publisher())]

        times.append(times_mysql)
        print(times_mysql)
        with open("./data/times.txt", "a") as file:
            file.write(str(times) + "\n")

    def generate_stats(self):
        queries = ['Query 1', 'Query 2', 'Query 3', 'Query 4', 'Query 5', 'Query 6']
        data_bases = ['MongoDB', 'Redis', 'SQL']
        # times = self.get_times_for_stats()

        mongo_color = '#9EE493'
        mysql_color = (0.584, 0.745, 0.812)
        redis_color = '#F39B93'

        with open('data/times.txt', 'r') as file:
            times = eval(file.read())

        mongo_values = times[0]
        redis_values = times[1]
        sql_values = times[2]

        plt.figure(figsize=(8, 8))

        width = 0.2
        offset = 0.2
        bottom_margin = 0.50

        plt.subplots_adjust(bottom=bottom_margin)

        x = np.arange(len(queries))

        # Generowanie słupków dla każdej bazy danych
        plt.bar(x - offset, mongo_values, width=width, label='MongoDB', color=mongo_color)
        plt.bar(x, redis_values, width=width, label='Redis', color=redis_color)
        plt.bar(x + offset, sql_values, width=width, label='SQL', color=mysql_color)

        plt.xlabel('Queries')
        plt.ylabel('Time [s]')
        plt.title('Execution time of queries for databases')
        plt.xticks([j + (len(data_bases) - 1) * offset / 2 for j in range(len(queries))], queries)  # Etykiety osi X
        plt.legend()

        # Dodanie tekstu pod wykresem
        plt.text(0, -0.20, 'Legend:', transform=plt.gca().transAxes, ha='left', fontsize=12)
        plt.text(0, -0.30, 'Query 1: Select all', transform=plt.gca().transAxes, ha='left', fontsize=10)
        plt.text(0, -0.40, 'Query 2: Select authors', transform=plt.gca().transAxes, ha='left', fontsize=10)
        plt.text(0, -0.50, 'Query 3: Count books by publisher', transform=plt.gca().transAxes, ha='left', fontsize=10)
        plt.text(0, -0.60, 'Query 4: Count words in titles', transform=plt.gca().transAxes, ha='left', fontsize=10)
        plt.text(0, -0.70, 'Query 5: Count avg publisher books', transform=plt.gca().transAxes, ha='left', fontsize=10)
        plt.text(0, -0.80, 'Query 6: Count median for books count by publisher', transform=plt.gca().transAxes,
                 ha='left', fontsize=10)

        plt.show()

    def close_window(self):
        self.clean_gui()
        self.root.destroy()


gui = Gui()
gui.root.protocol("WM_DELETE_WINDOW", gui.close_window)
gui.root.mainloop()

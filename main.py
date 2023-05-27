from datetime import datetime
from script.mongo_script import Mongo
from script.redis_script import Redis

def count_time(function):
    start_time = datetime.now()
    function()
    end_time = datetime.now()

    time = end_time - start_time
    return time


if __name__ == '__main__':
    mongo_client = Mongo()
    # mongo_client.create_mongo()
    # mongo_client.close_mongo()

    # time_select_mongo = count_time(lambda: mongo_client.select_mongo(author_name ='O\'Ryan, Ellie'))
    # print("Time for MONGODB: select:", time_select_mongo)

    time_select_mongo = count_time(lambda: mongo_client.delete_mongo(publication_year ='1930'))
    # print("Time for MONGODB: update:", time_select_mongo)

    # time_select_mongo = count_time(lambda: mongo_client.select_mongo('Lyon, Sidney Elizabeth, 1846-'))
    a= mongo_client.select_mongo('Lyon, Sidney Elizabeth, 1846-')
    print(a)


    # redis_client = Redis()
    # redis_client.create_redis()
    # redis_client.close_redis()

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
    # mongo_client = Mongo()
    # mongo_client.create_mongo()
    redis_client = Redis()
    # redis_client.create_redis()

    # time_select_mongo = count_time(lambda: mongo_client.select_mongo('Michael Crichton'))
    # print("Time for MONGODB: select:", time_select_mongo)

    # time_insert_mongo = count_time(lambda: mongo_client.insert_mongo())
    # print("Time for MONGODB: insert:", time_insert_mongo)

    # time_update_mongo = count_time(lambda: mongo_client.update_mongo('1930'))
    # print("Time for MONGODB: update:", time_update_mongo)

    # time_delete_mongo = count_time(lambda: mongo_client.delete_mongo('1930'))
    # print("Time for MONGODB: update:", time_delete_mongo)

    ############################################################

    # time_select_redis = count_time(lambda: redis_client.select_redis('London, Julia'))
    # print("Time for REDIS: select:", time_select_redis)

    # redis_client.clear_redis()

    time_select_all_redis = count_time(lambda: redis_client.select_all_redis())
    print(redis_client.select_all_redis())
    # print("Time for REDIS: select:", time_select_all_redis)

    # time_insert_redis = count_time(lambda: redis_client.insert_redis())
    # print("Time for REDIS: insert:", time_insert_redis)

    # time_select_all_redis = count_time(lambda: redis_client.select_all_redis())
    # print(redis_client.select_all_redis())
    # print("Time for REDIS: select:", time_select_all_redis)

    # time_select_redis = count_time(lambda: redis_client.select_redis('John Doe'))
    # print("Time for REDIS: select:", time_select_redis)

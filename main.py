from datetime import datetime
from script.mongo_script import Mongo
from script.redis_script import Redis
from script.sql_script import MySql


def count_time(function):
    start_time = datetime.now()
    function()
    end_time = datetime.now()

    time = end_time - start_time
    return time


if __name__ == '__main__':
    # mongo_client = Mongo()
    # mongo_client.create_mongo()

    # redis_client = Redis()
    # redis_client.create_redis()

    sql_client = MySql()
    sql_client.create_single_tables()
    sql_client.create_connection_table()

    ### MONGO ###

    # time_select_mongo = count_time(lambda: mongo_client.select('Michael Crichton'))
    # print("Time for MONGODB: select:", time_select_mongo)
    #
    # time_insert_mongo = count_time(lambda: mongo_client.insert())
    # print("Time for MONGODB: insert:", time_insert_mongo)
    #
    # time_update_mongo = count_time(lambda: mongo_client.update('1930'))
    # print("Time for MONGODB: update:", time_update_mongo)
    #
    # time_delete_mongo = count_time(lambda: mongo_client.delete('1930'))
    # print("Time for MONGODB: update:", time_delete_mongo)

    ### REDIS ###

    # redis_client.clear_redis()

    # time_select_redis = count_time(lambda: redis_client.select('John Doe'))
    # print("Time for REDIS: select:", time_select_redis)
    #
    # time_select_all_redis = count_time(lambda: redis_client.select_all())
    # print(redis_client.select_all())
    # print("Time for REDIS: select all:", time_select_all_redis)
    #
    # time_insert_redis = count_time(lambda: redis_client.insert())
    # print("Time for REDIS: insert:", time_insert_redis)
    #
    # time_update_redis = count_time(lambda: redis_client.update('2022'))
    # print("Time for REDIS: update:", time_select_redis)
    #
    # time_delete_redis = count_time(lambda: redis_client.delete('2022'))
    # print("Time for REDIS: delete:", time_select_redis)

    ### MYSQL ###

    # TODO
    # sql_client.select_all()
    # print(a)

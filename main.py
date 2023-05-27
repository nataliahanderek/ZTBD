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
    redis_client = Redis()



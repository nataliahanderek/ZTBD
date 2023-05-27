from script.mongo_script import Mongo
from script.redis_script import Redis

if __name__ == '__main__':
    mongo_client = Mongo()
    redis_client = Redis()


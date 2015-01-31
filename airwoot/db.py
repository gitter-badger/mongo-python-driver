import sys
import json
import redis
import settings
import argparse

def get_redis_connection():
    return redis.StrictRedis(host=settings.REDIS_HOST,
                             port=settings.REDIS_PORT,
                             db=settings.REDIS_DB)

def add_operation(collection_name, query):
    pipe = CONN.pipeline()
    pipe.sadd(collection_name, json.dumps(query))
    pipe.execute()

CONN = get_redis_connection()

def is_elem_in_set(conn, collname, column):
    for elem in conn.smembers(collname):
        elem = json.loads(elem)

        if not isinstance(elem, dict):
            continue

        if column in elem:
            return True

        for key, val in elem.iteritems():
            if not key.startswith("$"):
                continue

            if isinstance(val, dict):
                if column in val:
                    return True

            elif isinstance(val, list):
                for inner_dict in val:
                    if column in inner_dict:
                        return True

    return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Is the key used ever?')
    parser.add_argument('key', metavar="key", type=str,
                        help='column name that has to be searched')

    parser.add_argument('--collection', dest='collection', type=str,
                        help='collection that key should be searched in')

    args = parser.parse_args()

    is_present = False

    if args.collection:
        is_present = is_elem_in_set(CONN, args.collection, args.key)
    else:
        for coll in CONN.scan_iter():
            is_present = is_elem_in_set(CONN, coll, args.key)

    sys.exit(1) if is_present else 1

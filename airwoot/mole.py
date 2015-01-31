from bson.son import SON
from bson.binary import OLD_UUID_SUBTYPE
_INSERT = 0
_UPDATE = 1
_DELETE = 2
_QUERY = 4

def make_redis_record(collection_name, operation, query):
    print "%s[%s] -> %s" % (collection_name, operation, query)

def log_batch_operation(fn):
    op_keys = {
        _INSERT: "insert",
        _UPDATE: "update",
        _DELETE: "delete"
    }

    def inner(namespace, operation, command, docs, *args, **kwargs):
        cmd_dict = command.to_dict()
        collection_name = cmd_dict[op_keys[operation]]

        if isinstance(docs, (list, tuple)):
            for doc in docs:
                query = doc.to_dict()["q"]
                make_redis_record(collection_name, operation, query)

        elif hasattr(docs, "next"):
            pass

        else:
            query = docs.to_dict()["q"]
            make_redis_record(collection_name, operation, query)

        return fn(namespace, operation, command, docs, *args, **kwargs)
    return inner

def log_update(fn):
    def inner(collection_name, upsert, multi, spec, doc, *args, **kwargs):
        make_redis_record(collection_name, _UPDATE, spec)
        return fn(collection_name, upsert, multi, spec, doc, *args, **kwargs)
    return inner

def log_query(fn):
    def inner(options, namespace, num_to_skip,
              num_to_return, query, *args, **kwargs):

        if isinstance(query, SON):
            collection_name = query.values()[0]
            _query = query.to_dict()
            if _query.get("query"):
                make_redis_record(collection_name, _QUERY, _query["query"])

            elif _query.get("pipeline"):
                for q in _query["pipeline"]:
                    if q.get("$match"):
                        make_redis_record(collection_name, _QUERY, q["$match"])

        return fn(options, namespace, num_to_skip,
                  num_to_return, query, *args, **kwargs)
    return inner


def log_delete(fn):
    def inner(collection_name, spec, *args, **kwargs):
        make_redis_record(collection_name, _DELETE, spec)
        return fn(collection_name, spec, *args, **kwargs)
    return inner

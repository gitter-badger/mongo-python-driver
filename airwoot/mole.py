from bson.binary import OLD_UUID_SUBTYPE
_INSERT = 0
_UPDATE = 1
_DELETE = 2

def make_redis_record(namespace, collection_name, operation, query):
    print "%s::%s[%s] -> %s" % (
        namespace, collection_name, operation, query
    )

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
                make_redis_record(namespace, collection_name, operation, query)

        elif hasattr(docs, "next"):
            pass

        else:
            query = docs.to_dict()["q"]
            make_redis_record(namespace, collection_name, operation, query)

        return fn(namespace, operation, command, docs, *args, **kwargs)
    return inner

def log_update(fn):
    def inner(collection_name, upsert, multi, spec, doc, *args, **kwargs):
        return fn(collection_name, upsert, multi, spec, doc, *args, **kwargs)
    return inner

def log_query(fn):
    def inner(options, collection_name, num_to_skip,
              num_to_return, query, *args, **kwargs):

        return fn(options, collection_name, num_to_skip,
                  num_to_return, query, *args, **kwargs)
    return inner


def log_delete(fn):
    def inner(collection_name, spec, *args, **kwargs):
        return fn(collection_name, spec, *args, **kwargs)
    return inner

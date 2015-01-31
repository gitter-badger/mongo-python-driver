REDIS_HOST="127.0.0.1"
REDIS_PORT=6379
REDIS_DB=0

try:
    from local_settings import *
except ImportError, e:
    print "Ignoring local_settings import. Reason: [%s]" % e

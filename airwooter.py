from pymongo import MongoClient

MONGO_HOST = "127.0.0.1"
MONGO_PORT = 27018
DBNAME = "airwoot"

def get_connection():
    return MongoClient(host=MONGO_HOST,
                       port=MONGO_PORT)

def seed_data():
    con = get_connection()
    con.drop_database(DBNAME)

    db = con[DBNAME]

    #Find inserted queries
    query = {"$and": [{"op": "i"}]}
    print "Count"
    db.test.find(query).count()

    #Insert
    print "Inserts"
    db.test.insert({"op": "i"})
    db.test.insert({"op": "a"})
    db.test.insert({"op": "e"})
    db.test.insert({"op": "o"})
    db.test.insert({"op": "u"})

    print "Distinct"
    db.test.find({"op": "i"}).distinct("_id")

    #Update
    print "Update. multi=false"
    db.test.update({"op": {"$ne": "i"}}, {"$set": {"op": "i"}})

    print "Update multi=True"
    db.test.update({"op": {"$ne": "i"}}, {"$set": {"op": "i"}}, multi=True)

    print "Aggregation"
    db.test.aggregate([
        {"$match": {"op": "i"}},
        {"$project": {"op": 1}},
    ])


    print "Remove"
    db.test.remove({"op": "i"})

if __name__ == "__main__":
    seed_data()

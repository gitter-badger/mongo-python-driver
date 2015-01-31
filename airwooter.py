from pymongo import MongoClient

con = MongoClient(port=27018)

DBNAME = "airwoot"
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



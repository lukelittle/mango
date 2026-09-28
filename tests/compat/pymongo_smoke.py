"""Smoke test with PyMongo (a second official driver, which also exercises
the legacy OP_QUERY handshake). Usage: python pymongo_smoke.py mongodb://host:port/"""
import sys
import datetime
from pymongo import MongoClient, ASCENDING, DESCENDING, ReturnDocument
from pymongo.errors import DuplicateKeyError, OperationFailure

uri = sys.argv[1] if len(sys.argv) > 1 else "mongodb://127.0.0.1:27017/?directConnection=true"
client = MongoClient(uri, serverSelectionTimeoutMS=10000)
client.drop_database("pysmoke")
db = client.pysmoke
c = db.people

c.insert_many([
    {"_id": i, "name": f"p{i}", "age": 20 + i % 30, "city": ["NYC", "SF", "LA"][i % 3],
     "tags": ["a", "b"] if i % 2 else ["c"], "joined": datetime.datetime(2024, 1, 1) + datetime.timedelta(days=i)}
    for i in range(300)
])
assert c.count_documents({}) == 300
assert c.count_documents({"city": "SF", "age": {"$gte": 40}}) == len([i for i in range(300) if i % 3 == 1 and 20 + i % 30 >= 40])

c.create_index([("city", ASCENDING), ("age", DESCENDING)])
c.create_index("name", unique=True)
try:
    c.insert_one({"name": "p1"})
    raise AssertionError("expected duplicate key error")
except DuplicateKeyError:
    pass

top = list(c.find({"city": "LA"}, {"_id": 0, "name": 1, "age": 1}).sort([("age", -1), ("name", 1)]).limit(3))
assert len(top) == 3 and top[0]["age"] == 49, top

res = c.update_many({"tags": "a"}, {"$addToSet": {"tags": "z"}, "$currentDate": {"touched": True}})
assert res.matched_count == 150 and res.modified_count == 150
doc = c.find_one_and_update({"_id": 7}, {"$inc": {"age": 100}}, return_document=ReturnDocument.AFTER)
assert doc["age"] == 20 + 7 + 100

pipeline = [
    {"$match": {"age": {"$lt": 30}}},
    {"$group": {"_id": "$city", "n": {"$sum": 1}, "avgAge": {"$avg": "$age"}, "names": {"$push": "$name"}}},
    {"$project": {"n": 1, "avgAge": {"$round": ["$avgAge", 1]}, "first": {"$arrayElemAt": ["$names", 0]}}},
    {"$sort": {"_id": 1}},
]
out = list(c.aggregate(pipeline))
assert [o["_id"] for o in out] == ["LA", "NYC", "SF"], out

by_month = list(c.aggregate([
    {"$group": {"_id": {"$month": "$joined"}, "n": {"$sum": 1}}},
    {"$sort": {"_id": 1}},
]))
assert by_month[0] == {"_id": 1, "n": 31}, by_month

# $lookup across collections
db.orders.insert_many([{"person": i % 10, "amount": i} for i in range(50)])
joined = list(c.aggregate([
    {"$match": {"_id": {"$lt": 3}}},
    {"$lookup": {"from": "orders", "localField": "_id", "foreignField": "person", "as": "orders"}},
    {"$project": {"count": {"$size": "$orders"}}},
]))
assert [j["count"] for j in joined] == [5, 5, 5], joined

assert sorted(c.distinct("city")) == ["LA", "NYC", "SF"]
assert c.delete_many({"age": {"$gte": 45}}).deleted_count > 0

# Bulk write with mixed operations
from pymongo import InsertOne, UpdateOne, DeleteOne, ReplaceOne
r = c.bulk_write([InsertOne({"_id": 1000}), UpdateOne({"_id": 1000}, {"$set": {"x": 1}}),
                  ReplaceOne({"_id": 1000}, {"y": 2}), DeleteOne({"_id": 1000})])
assert (r.inserted_count, r.modified_count, r.deleted_count) == (1, 2, 1), r.bulk_api_result

# Errors carry MongoDB codes
try:
    c.find_one({"$where": "1"})
    raise AssertionError("expected error")
except OperationFailure as e:
    assert e.code == 238

names = sorted(db.list_collection_names())
assert names == ["orders", "people"], names
indexes = sorted(ix["name"] for ix in c.list_indexes())
assert indexes == ["_id_", "city_1_age_-1", "name_1"], indexes
client.drop_database("pysmoke")
print("pymongo smoke test passed")

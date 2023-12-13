import msgpack
from pymongo import MongoClient
import json


def get_collection(conUrl):
    client = MongoClient(conUrl)
    db = client["task1"]
    return db.person_data


def insert_data(data, collection):
    collection.insert_many(data)


def load_data(fileName):
    with open(fileName, "rb") as file:
        return msgpack.loads(file.read())


def write_sorted_salary(collection):
    persons = list(collection.find({}).limit(10).sort({"salary": -1}))

    with open("sorted_salary.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(persons, ensure_ascii=False, default=str))


def write_filtered_age(collection):
    persons = list(collection.find({"age": {"$lt": 30}}, limit=15).sort({"salary": -1}))

    with open("filtered_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(persons, ensure_ascii=False, default=str))


def write_filtered_city(collection):
    query = {"city": "Вроцлав",
             "job": {"$in": ["Врач", "Бухгалтер", "Водитель"]}
             }
    persons = list(collection.find(query, limit=10).sort({"age": 1}))

    with open("filtered_city.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(persons, ensure_ascii=False, default=str))


def write_complex_filtered(collection):
    query = {
        "age": {"$gt": 18, "$lt": 35},
        "year": {"$in": [2019, 2020, 2021, 2022]},
        "$or": [{"salary": {"$gt": 50000, "$lte": 75000}},
                {"salary": {"$gt": 125000, "$lt": 150000}}]
    }
    _len = len(list(collection.find(query)))

    with open("complex_filter.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(_len, ensure_ascii=False, default=str))


data = load_data("task_1_item.msgpack")
collection = get_collection("mongodb://docker:mongopw@localhost:55000")

if collection.count_documents({}) == 0:
    insert_data(data, collection)

write_sorted_salary(collection)
write_filtered_city(collection)
write_filtered_age(collection)
write_complex_filtered(collection)

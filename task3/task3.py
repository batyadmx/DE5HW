import json
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
        return json.loads(file.read())


def delete_salary(collection):
    query = {"$or": [{"salary": {"$lt": 25000}},
                     {"salary": {"$gt": 175000}}]}

    collection.delete_many(query)


def increment_age(collection):
    collection.update_many({}, {"$inc": {"age": 1}})


def increase_salary_for_job(collection, multiplyer):
    collection.update_many({"job": {"$in": ["Программист"]}},
                           {"$mul": {"salary": multiplyer}})


def increase_salary_in_city(collection, multiplier):
   collection.update_many({"city": {"$in": ["Москва"]}},
                          {"$mul": {"salary": multiplier}})


def increase_salary_complex(collection, multiplier):
    collection.update_many({"$and": [{"city": {"$in": ["Москва"]}},
                                           {"job": {"$in": ["Программист"]}}]},
                                 {"$mul": {"salary": multiplier}})

def delete_job(collection):
    collection.delete_many({"job": "Бухгалтер"})


data = load_data("task_3_item.json")
collection = get_collection("mongodb://docker:mongopw@localhost:55000")

delete_salary(collection)
increment_age(collection)
increase_salary_for_job(collection, 1.05)
increase_salary_in_city(collection, 1.07)
increase_salary_complex(collection, 1.10)
delete_job(collection)
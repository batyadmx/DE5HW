import json
from pymongo import MongoClient
import pickle


def get_collection(conUrl):
    client = MongoClient(conUrl)
    db = client["task1"]
    return db.person_data


def insert_data(data, collection):
    collection.insert_many(data)


def load_data(fileName):
    with open(fileName, "rb") as file:
        return pickle.loads(file.read())


def salary_aggregate(collection):
    query = [
        {"$group": {"_id": "salary",
                    "max": {"$max": "$salary"},
                    "min": {"$min": "$salary"},
                    "avg": {"$avg": "$salary"}}}
    ]
    items = list(collection.aggregate(query))

    with open("salary_aggregate.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


def aggregate_job(collection):
    query = [{"$group": {"_id": "$job", "sum": {"$sum": 1}}}]
    items = list(collection.aggregate(query))

    with open("aggregate_job.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


def aggregate_min_max_avg_info(groupBy, aggregateBy):
    query = \
        [{"$group":
              {"_id": f"${groupBy}",
               "max_salary": {"$max": f"${aggregateBy}"},
               "min_salary": {"$min": f"${aggregateBy}"},
               "avg_salary": {"$avg": f"${aggregateBy}"}
               }
          }]
    items = list(collection.aggregate(query))

    with open(f"aggregate_{groupBy}_{aggregateBy}_info.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


def max_salary_min_age(collection):
    query = [
        {"$group": {"_id": "$age",
                    "max_salary": {"$max": "$salary"}}
        },
        {"$sort": {"_id": 1}}]
    items = list(collection.aggregate(query))[0]

    with open("max_salary_min_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


def min_salary_max_age(collection):
    query = [
        {"$group": {"_id": "$age",
                    "min_salary": {"$min": "$salary"}}
         },
        {"$sort": {"_id": -1}}
    ]
    items = list(collection.aggregate(query))[0]

    with open("min_salary_max_age.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))

def aggregate_city_filter_salary(collection):
    query = [
        {"$match": {"salary": {"$gt": 50000}}},
        {"$group": {"_id": "$city",
                    "max_age": {"$max": "$age"},
                    "min_age": {"$min": "$age"},
                    "avg_age": {"$avg": "$age"}}
         },
        {"$sort": {"_id": 1}}
    ]

    items = list(collection.aggregate(query))

    with open("aggregate_city_filter_salary.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


def aggregate_with_filter(collection, groupBy):
    query = [
        {"$match": {"$or": [{"age": {"$gt": 18, "$lte": 25}},
                            {"age": {"$gt": 50, "$lt": 65}}]}},
        {"$group": {"_id": f"${groupBy}",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
         },
        {"$sort": {"_id": 1}}
    ]

    items = list(collection.aggregate(query))

    with open(f"aggregate_{groupBy}_with_filter.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))

def salary_job_Moscow(collection):
    query = [
        {"$match": {"city": "Москва"}},
        {"$group": {"_id": "$job",
                    "max_salary": {"$max": "$salary"},
                    "min_salary": {"$min": "$salary"},
                    "avg_salary": {"$avg": "$salary"}}
         },
        {"$sort": {"_id": 1}}
    ]

    items = list(collection.aggregate(query))

    with open(f"moscow_salaries.json", "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))


data = load_data("task_2_item.pkl")
collection = get_collection("mongodb://docker:mongopw@localhost:55000")

salary_aggregate(collection)
aggregate_job(collection)
aggregate_min_max_avg_info("city", "salary")
aggregate_min_max_avg_info("job", "salary")
aggregate_min_max_avg_info("city", "age")
aggregate_min_max_avg_info("job", "age")
max_salary_min_age(collection)
min_salary_max_age(collection)
aggregate_with_filter(collection, "city")
aggregate_with_filter(collection, "job")
aggregate_with_filter(collection, "age")
salary_job_Moscow(collection)

import json
from pymongo import MongoClient
import csv


def get_collection(conUrl):
    client = MongoClient(conUrl)
    db = client["task4"]
    return db.organizations


def insert_data(data, collection):
    collection.insert_many(data)


def load_data(fileName):
    with open(fileName, newline='') as file:
        data = list(csv.DictReader(file))
    for doc in data:
        doc["Number of employees"] = int(doc["Number of employees"])
        doc["Founded"] = int(doc["Founded"])
    return data

def save_as_json(path, items):
    with open(path, "w", encoding="utf-8") as file:
        file.write(json.dumps(items, ensure_ascii=False, default=str))

def get_all_orgs():
    return list(collection.distinct("Name"))

def get_all_orgs_located_in_country(country):
    query = [{"$match": {"Country" : country}}]

    return list(collection.aggregate(query))

def get_all_orgs_filter_employees(amount):
    query = [{"$match": {"Number of employees" : {"$gt" : amount}}}]
    return list(collection.aggregate(query))

def aggregate_industry_count():
    query = [{"$group": {"_id": "$Industry", "sum": {"$sum": 1}}}]
    return list(collection.aggregate(query))

def aggregate_founded_year_count():
    query = [{"$group": {"_id": "$Founded", "sum": {"$sum": 1}}}
             , {"$sort" : {"sum" : -1}}]
    return list(collection.aggregate(query))

def aggregate_industry_min_max_avg_employees():

    query = [{"$group": {"_id": "$Industry",
                         "max": {"$max": "$Number of employees"},
                         "min": {"$min": "$Number of employees"},
                         "avg": {"$avg": "$Number of employees"}}}]
    return list(collection.aggregate(query))

def aggregate_country_min_max_avg_employees():

    query = [{"$group": {"_id": "$Country",
                         "max": {"$max": "$Number of employees"},
                         "min": {"$min": "$Number of employees"},
                         "avg": {"$avg": "$Number of employees"}}}]
    return list(collection.aggregate(query))

def delete_industry():
    collection.delete_many({"Industry": "Transportation"})

def delete_country():
    collection.delete_many({"Country": "Croatia"})

def delete_when_founded_earlier_2000():
    collection.delete_many({"Founded": {"$lt" : 2000}})

def delete_it_where_employees_more_5000():
    collection.delete_many({"$and" : [
        {"Industry": "Computer Software / Engineering"},
        {"Number of employees" : {"$gt" : 5000}}]})


collection = get_collection("mongodb://docker:mongopw@localhost:55000")

if collection.count_documents({}) == 0:
    insert_data(load_data("organizations-10000.csv"), collection)

save_as_json("organization_names.json", get_all_orgs())
save_as_json("orgs_in_uk.json", get_all_orgs_located_in_country("United Kingdom"))
save_as_json("orgs_in_russia.json", get_all_orgs_located_in_country("Russia"))
save_as_json("empl_more_5000.json", get_all_orgs_filter_employees(5000))
save_as_json("aggregate_industry_count.json", aggregate_industry_count())
save_as_json("aggregate_founded_year_count.json", aggregate_founded_year_count())
save_as_json("aggregate_industry_min_max_avg_employees.json", aggregate_industry_min_max_avg_employees())
save_as_json("aggregate_country_min_max_avg_employees.json", aggregate_country_min_max_avg_employees())

delete_industry()
delete_country()
delete_when_founded_earlier_2000()
delete_it_where_employees_more_5000()


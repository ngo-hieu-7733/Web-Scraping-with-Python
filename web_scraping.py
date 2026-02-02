from dotenv import load_dotenv, find_dotenv
import os
import pprint
from pymongo import MongoClient

load_dotenv(find_dotenv()) # load .env file

connection_str = os.environ.get("MONGO_CONNECTION_STR") # get the variable in .env file

client = MongoClient(connection_str) # connect to mongodb

try:
    database = client.get_database("it4409")
    users = database.get_collection("users")

    query = { "name": "do trung hieu" }
    user = users.find_one(query)
    print(user)
    client.close()
except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)


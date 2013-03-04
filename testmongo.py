from pymongo import mongoclient

connection = MongoClient()
db = connection['facebook']
collection = db['sandyspets']
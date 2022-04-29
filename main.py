from pymongo import MongoClient

if __name__ == '__main__':
    uri = "mongodb://50.17.201.202:27021"
    client = MongoClient(uri)
    db = client.config
    collection = db.collections
    documents = collection.find()
    for document in documents:
        print(document)
    client.close()

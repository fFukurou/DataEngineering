from personal_info import MONGODB_PASSWORD
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

# ------------------- FUNCTIONS ------------------------


def connect_mongo(uri: str) -> MongoClient:
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
        return client
    except Exception as e:
        print(e)


def create_connect_db(client: MongoClient, db_name: str):
    db = client[db_name]
    return db


def create_connect_collection(db, col_name: str):
    collection = db[col_name]
    return collection


def extract_api_data(url: str):
    response = requests.get(url)
    return response.json()


def insert_data(col, data) -> int:
    docs = col.insert_many(data)
    return len(docs.inserted_ids)


# ------------------- VARIABLES ------------------------

uri = f"mongodb+srv://fFukurou:{MONGODB_PASSWORD}@alura-pipeline.bwbp3.mongodb.net/?retryWrites=true&w=majority&appName=Alura-Pipeline"
api_url = "https://labdados.com/produtos"
database_name = "db_produtos_desafio"
collection_name = "produtos"


# ------------------- SCRIPT EXECUTION --------------------------
if __name__ == "__main__":
    # Creating a connection with MongoDB cluster
    client = connect_mongo(uri)

    # Creating and returning a database object
    db = create_connect_db(client, database_name)

    # Creating and returning collection
    collection = create_connect_collection(db, collection_name)

    # Requesting and returning data in the json format
    data_json = extract_api_data(api_url)
    print(f"Number of extracted data: {len(data_json)}")

    # Inserting the data into our cluster-> database -> collection and returning number of inserted entries
    inserted_entries_num = insert_data(collection, data_json)
    print(f"Number of inserted entries: {inserted_entries_num}")

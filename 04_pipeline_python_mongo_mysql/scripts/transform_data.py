from personal_info import MONGODB_PASSWORD
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests
import pandas as pd
from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection


# ------------------- FUNCTIONS ------------------------

def visualize_collection(col):
    i = 0
    for doc in col.find():
        if i < 5:
            print(doc)
            i += 1
        else:
            break


def rename_column(col, field_name, new_name):
    col.update_many({}, {'$rename': {field_name: new_name}})


def select_category(col, category):
    lista_categ = []
    for doc in col.find({'Categoria do Produto': category}):
        lista_categ.append(doc)

    return lista_categ


def make_regex(col, filter, regex):
    lista_regex = []
    for doc in col.find({filter: {"$regex": regex}}):
        lista_regex.append(doc)

    return lista_regex


def create_dataframe(lista):
    df = pd.DataFrame(lista)
    return df


def format_date(df):
    df['Data da Compra'] = pd.to_datetime(df['Data da Compra'], format="%d/%m/%Y")

    df['Data da Compra'] = df['Data da Compra'].dt.strftime("%Y-%m-%d")


def save_csv(df, path):
    df.to_csv(path, index=False)


def close_connection(client):
    client.close()


# ------------------- VARIABLES ------------------------
uri = f"mongodb+srv://fFukurou:{MONGODB_PASSWORD}@alura-pipeline.bwbp3.mongodb.net/?retryWrites=true&w=majority&appName=Alura-Pipeline"
database_name = 'db_produtos_desafio'
collection_name = 'produtos'


# ------------------- SCRIPT EXECUTION --------------------------
# Connect to MongoDB, read data, rename columns, select data based on category, select data based on a filter and its regex


if __name__ == "__main__":
    # Creating a connection with MongoDB cluster
    client = connect_mongo(uri)

    # Creating and returning a database object
    db = create_connect_db(client, database_name)

    # Creating and returning collection
    collection = create_connect_collection(db, collection_name)

    # Printing data
    visualize_collection(collection)

    # Renaming fields
    rename_column(collection, 'lat', 'Latitude')
    rename_column(collection, 'lon', 'Longitude')

    # Selecing a specific product category and filter regex
    lista_livros = select_category(collection, 'livros')
    lista_produtos = make_regex(collection, "Data da Compra", '/202[1-9]')

    # Creating dataframes out of the list of dicts
    df_livros = create_dataframe(lista_livros)
    df_produtos = create_dataframe(lista_produtos)

    # Formatting the Date on the dataframes (dd/mm/yyyy) --> (yyyy-mm-dd)
    format_date(df_livros)
    format_date(df_produtos)

    # Saving the formatted dataframes into csv files
    save_csv(df_livros, 'data_desafio/tabela_livros.csv')
    save_csv(df_produtos, 'data_desafio/tabela_2021_em_diante.csv')

    # Closing the connection
    close_connection(client)

import pandas as pd
import mysql.connector
import requests
from personal_info import *

# ------------------- FUNCTIONS ------------------------


def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host=host_name,
        user=user_name,
        password=pw
    )

    return cnx


def create_cursor(cnx):
    return cnx.cursor()


def create_database(cursor, db_name):
    sql = f"CREATE DATABASE IF NOT EXISTS {db_name};"

    cursor.execute(sql)


def show_databases(cursor):
    cursor.execute("SHOW DATABASES;")

    print(f"DATABASES:\n")

    for db in cursor:
        print(db)


def create_product_table(cursor, db_name, tb_name):
    sql = f"""
    CREATE TABLE IF NOT EXISTS {db_name}.{tb_name}(
        id VARCHAR(100),
        Produto VARCHAR(100),
        Categoria_Produto VARCHAR(100),
        Preco FLOAT(10,2),
        Frete FLOAT(10,2),
        Data_Compra DATE,
        Vendedor VARCHAR(100),
        Local_Compra VARCHAR(100),
        Avaliacao_Compra INT,
        Tipo_Pagamento VARCHAR(100),
        Qntd_Parcelas INT,
        Latitude FLOAT(10,2),
        Longitude FLOAT(10,2),
        
        PRIMARY KEY (id)
    );   
    """
    cursor.execute(sql)


def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name};")
    cursor.execute("SHOW TABLES;")

    print(f"TABLES IN THE {db_name} DATABASE:\n")

    for table in cursor:
        print(table)


def csv_to_dict(path):
    df = pd.read_csv(path)
    return df


def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista_dados = [tuple(row) for i, row in df.iterrows()]

    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

    cursor.executemany(sql, lista_dados)
    cnx.commit()
    print(cursor.rowcount, "dados inseridos")


def close_all(connection, cursor):
    cursor.close()
    connection.close()


# ------------------- VARIABLES ------------------------

database_name = "alura_db_produtos_desafio"
livros_table = "table_livros"


# ------------------- SCRIPT EXECUTION --------------------------
# Connect to server, create a cursor, database, and table, read from csv, transform it into DataFrame, and insert into MySQL table
if __name__ == "__main__":
    # Creating connection
    cnx = connect_mysql(MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD)

    # Creating cursor
    cursor = create_cursor(cnx)

    # Creating the database
    create_database(cursor, database_name)

    # Showing all databases
    show_databases(cursor)

    # Creating the table
    create_product_table(cursor, database_name, livros_table)

    # Showing all tables in the created database
    show_tables(cursor, database_name)

    # Reading from CSV and creating DataFrame
    df = csv_to_dict("data/tabela_livros.csv")

    # Inserting the data into the table
    add_product_data(cnx, cursor, df, database_name, livros_table)

    # Closing the connection and cursor
    close_all(cnx, cursor)

import csv
import os
from hashlib import sha256
from datetime import datetime
import logging
from DB_connection import connect_to_database
import configparser
import mysql.connector

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = configparser.ConfigParser()
config.read('config.ini')
output_dir = config.get('output', 'output_dir')

def determine_batch_size(column_names, max_size=500):
    """Determine the optimal batch size based on column sizes."""
    total_size = sum(len(col) for col in column_names)
    return 1000 if total_size < max_size else 100

def convert_to_integer(row, column_names):
    """Forcibly convert all columns to integers where possible."""
    for i, value in enumerate(row):
        try:
            row[i] = int(value)
        except ValueError:
            pass
    return row

def fetch_and_write_data():
    """Fetch data from the database and write to a CSV file."""
    connection = None
    cursor = None
    try:
        connection = connect_to_database()
        cursor = connection.cursor()
        
        # Check if data exists
        cursor.execute("SELECT COUNT(*) FROM poc.flipkart_dataset;")
        count = cursor.fetchone()[0]
        logging.info(f"Total records in 'flipkart_dataset': {count}")

        if count == 0:
            logging.warning("The table 'flipkart_dataset' is empty!")
            return

        # Fetch column names
        cursor.execute("SELECT * FROM flipkart_dataset LIMIT 1;")
        column_names = [i[0] for i in cursor.description]
        batch_size = determine_batch_size(column_names)
        offset = 0
        group_separator = '\u001D'

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        filename = os.path.join(output_dir, datetime.now().strftime("hashed_users_%Y%m%d_%H%M%S.csv"))

        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=group_separator)
            writer.writerow(column_names + ['Hashed_Product_Name'])

            while True:
                cursor.execute(f"SELECT * FROM flipkart_dataset LIMIT {batch_size} OFFSET {offset};")
                users = cursor.fetchall()
                if not users:
                    break
                for user in users:
                    row = list(user)
                    row = convert_to_integer(row, column_names)
                    product_name = row[column_names.index('product_name')]  # ensure 'product_name' exists
                    hashed_product_name = sha256(product_name.encode()).hexdigest()
                    writer.writerow(row + [hashed_product_name])
                offset += batch_size
                logging.info(f"Processed batch offset {offset}")

        logging.info(f"Data has been written to {filename} successfully.")
    except mysql.connector.Error as e:
        logging.error(f"Error while executing SQL query: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

if __name__ == "__main__":
    fetch_and_write_data()

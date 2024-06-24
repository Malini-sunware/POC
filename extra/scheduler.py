import mysql.connector
from hashlib import sha256
import csv
import schedule
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

config = {
    'user': 'root',
    'password': 'zxcvbnm,./123',
    'host': 'localhost',
    'database': 'poc',
    'raise_on_warnings': True
}

def convert_to_integer(row, column_names):
    """Forcibly convert all columns to integers where possible."""
    for i, value in enumerate(row):
        try:
            row[i] = int(value)
        except ValueError:
            pass  
    return row

def fetch_and_write_data():
    logging.info("Starting data fetch and write process")
    try:
        db_connection = mysql.connector.connect(**config)
        print("Successfully connected to the database")
    except mysql.connector.Error as e:
        print("Error while connecting to MySQL", e)
        return

    try:
        cursor = db_connection.cursor()
        cursor.execute("SELECT * FROM flipkart_dataset;")
        users = cursor.fetchall()
        
        column_names = [i[0] for i in cursor.description]
        group_separator = '\u001D'
        
        
        filename = datetime.now().strftime("scheduled_file_%Y%m%d_%H%M%S.csv")
        
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=group_separator)
            writer.writerow(column_names + ['Hashed_Product_Name'])
            
            for user in users:
                row = list(user)
                row = convert_to_integer(row, column_names)
                product_name = row[column_names.index('product_name')]
                hashed_product_name = sha256(product_name.encode()).hexdigest()
                writer.writerow(row + [hashed_product_name])
        
        logging.info(f"{filename} generated successfully.")
    except mysql.connector.Error as e:
        print("Error while executing SQL query", e)
    finally:
        if cursor:
            cursor.close()
        if db_connection:
            db_connection.close()


schedule.every().day.at("11:00").do(fetch_and_write_data)

print("Scheduler started...")

while True:
    schedule.run_pending()
    time.sleep(1)

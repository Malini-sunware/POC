import mysql.connector
from hashlib import sha256
import csv

config = {
    'user': 'root',
    'password': 'zxcvbnm,./123',
    'host': 'localhost',
    'database': 'poc',
    'raise_on_warnings': True
}

try:
    db_connection = mysql.connector.connect(**config)
    print("Successfully connected to the database")
except mysql.connector.Error as e:
    print("Error while connecting to MySQL", e)
    exit(1)

try:
    cursor = db_connection.cursor()
    cursor.execute("SELECT * FROM flipkart_dataset;")
    users = cursor.fetchall()
    
    column_names = [i[0] for i in cursor.description]
    
    
    group_separator = '\u001D'
    
    with open('hashed_users.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter=group_separator)
        
        writer.writerow( column_names + ['Hashed_Product_Name'])
        
        for unique_id, user in enumerate(users):
            row = list(user)
            product_name = row[column_names.index('product_name')]
            hashed_product_name = sha256(product_name.encode()).hexdigest()
            writer.writerow( row + [hashed_product_name])
    
    print("Data has been written to hashed_users.csv successfully.")
except mysql.connector.Error as e:
    print("Error while executing SQL query", e)
finally:
    cursor.close()
    db_connection.close()

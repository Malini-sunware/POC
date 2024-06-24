import mysql.connector
import configparser
import os
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_config():
    """Read database configuration from config.ini."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = {
        'user': config.get('database', 'user'),
        'password': os.getenv('DB_PASSWORD', config.get('database', 'password')),
        'host': config.get('database', 'host'),
        'database': config.get('database', 'database'),
        'raise_on_warnings': config.getboolean('database', 'raise_on_warnings')
    }
    logging.info(f"Database config: {db_config}")
    return db_config

def connect_to_database():
    """Establish a database connection."""
    db_config = get_db_config()
    logging.info(f"Attempting to connect to MySQL with config: {db_config}")
    try:
        connection = mysql.connector.connect(**db_config)
        logging.info("Successfully connected to the database")
        return connection
    except mysql.connector.Error as e:
        logging.error(f"Error while connecting to MySQL: {e}")
        raise Exception(f"Error while connecting to MySQL: {e}")

if __name__ == "__main__":
    try:
        logging.info(f"DB_PASSWORD from environment: {os.getenv('DB_PASSWORD')}")
        connect_to_database()
    except Exception as e:
        logging.error(e)

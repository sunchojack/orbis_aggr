import psycopg2
from psycopg2 import sql, OperationalError
import pickle
from cryptography.fernet import Fernet

# def load_password():
#     with open("password.pkl", "rb") as f:
#         data = pickle.load(f)
#     key = data["key"]
#     encrypted_password = data["password"]
#     cipher_suite = Fernet(key)
#     decrypted_password = cipher_suite.decrypt(encrypted_password).decode()
#     return decrypted_password

def create_connection(db_name, db_user, db_host, db_port):
    # db_password = load_password()
    db_password = "04101999Aa**"
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection

# Define your connection parameters
db_name = "orbis"
db_user = "postgres"
db_host = "172.16.7.6"  # Replace with your VM's IP address
db_port = "5432"  # Default PostgreSQL port

# Create a connection
connection = create_connection(db_name, db_user, db_host, db_port)

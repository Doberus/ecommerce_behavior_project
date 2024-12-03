import psycopg2
import pandas as pd

# Database connection parameters
DB_NAME = "ecommerce_behavior"
DB_USER = "postgres"
DB_PASSWORD = "Doberus@123"  
DB_HOST = "localhost"
DB_PORT = "5432"

# Load the dataset
# data_file = "D:/Projects/ecommerce_behavior_project/datasets/2019-Oct.csv"
data_file = "D:/Projects/ecommerce_behavior_project/datasets/2019-Nov.csv"
# Define chunk size
chunk_size = 100000  # Number of rows per chunk

# Connect to PostgreSQL
try:
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT
    )
    cursor = conn.cursor()
    print(f"Connected to the database. Processing file: {data_file}")

    # Process the dataset in chunks
    for chunk in pd.read_csv(data_file, chunksize=chunk_size):
        for index, row in chunk.iterrows():
            cursor.execute("""
                INSERT INTO user_behavior (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        conn.commit()
        print(f"Inserted {len(chunk)} rows from current chunk.")

    print(f"Data from {data_file} inserted successfully.")
    
except Exception as e:
    print("Error:", e)
finally:
    cursor.close()
    conn.close()

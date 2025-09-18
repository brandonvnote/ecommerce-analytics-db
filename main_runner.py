import os
import psycopg2
from dotenv import load_dotenv

from data.data_generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_reviews,
    generate_shipments
)

# Load environment variables
load_dotenv()

db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def main():
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            generate_customers(cur)
            generate_products(cur)
            generate_orders(cur)
            generate_reviews(cur)
            generate_shipments(cur)

        conn.commit()
    print("Fake data inserted successfully.")

if __name__ == "__main__":
    main()

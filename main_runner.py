import os
import psycopg2
from dotenv import load_dotenv

from data.data_generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_reviews,
    generate_shipments,
)

# Load environment variables
load_dotenv()

db_config = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}


def table_count(cur, table_name: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def main():
    print("Connecting to database...")
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Generate customers first to satisfy FK dependencies
            print("Generating customers...")
            generate_customers(cur, n=50)
            print(f"Customers total: {table_count(cur, 'Customers')}")

            # Generate products next
            print("Generating products...")
            generate_products(cur, n=30)
            print(f"Products total: {table_count(cur, 'Products')}")

            # Orders require valid customers and products
            print("Generating orders...")
            generate_orders(cur, n=100)
            print(f"Orders total: {table_count(cur, 'Orders')}")
            print(f"Order_Items total: {table_count(cur, 'Order_Items')}")

            # Reviews also require valid customers and products
            print("Generating reviews...")
            generate_reviews(cur, n=100)
            print(f"Reviews total: {table_count(cur, 'Reviews')}")

            # Shipments require valid orders
            print("Generating shipments...")
            generate_shipments(cur, n=100)
            print(f"Shipments total: {table_count(cur, 'Shipments')}")

        conn.commit()
        print("\nData generation complete.")


if __name__ == "__main__":
    main()
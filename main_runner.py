# File: main_runner.py
# Purpose: Client API for generating e-commerce database data.
# Handles database connection, user input, and calls into data_generators.

import os
import psycopg2
from dotenv import load_dotenv

from data.data_generators import (
    generate_customers,
    generate_products,
    generate_orders,
    generate_reviews,
    generate_shipments,
    curate_ids,
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
    """
    Return the number of rows in a given table.

    Args:
        cur: Database cursor.
        table_name (str): Name of the table.

    Returns:
        int: Row count.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def prompt_yes_no(message: str) -> bool:
    """
    Prompt the user for a yes/no response.

    Args:
        message (str): Prompt message.

    Returns:
        bool: True if user enters 'y', False if 'n'.
    """
    while True:
        resp = input(f"{message} (y/n): ").strip().lower()
        if resp in ("y", "n"):
            return resp == "y"
        print("Please enter 'y' or 'n'.")


def main():
    print("Connecting to database...")
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Customers
            if prompt_yes_no("Generate customers?"):
                n = int(input("How many customers? "))
                generate_customers(cur, n)
                print(f"Customers total: {table_count(cur, 'Customers')}")

            # Products
            if prompt_yes_no("Generate products?"):
                n = int(input("How many products? "))
                generate_products(cur, n)
                print(f"Products total: {table_count(cur, 'Products')}")

            # Orders
            if prompt_yes_no("Generate orders?"):
                ids = curate_ids(cur)
                n = int(input("How many orders? "))
                generate_orders(cur, ids["customers"], ids["products"], n)
                print(f"Orders total: {table_count(cur, 'Orders')}")
                print(f"Order_Items total: {table_count(cur, 'Order_Items')}")

            # Reviews
            if prompt_yes_no("Generate reviews?"):
                ids = curate_ids(cur)
                n = int(input("How many reviews? "))
                generate_reviews(cur, ids["customers"], ids["products"], n)
                print(f"Reviews total: {table_count(cur, 'Reviews')}")

            # Shipments
            if prompt_yes_no("Generate shipments?"):
                n = int(input("How many shipments? "))
                generate_shipments(cur, n=n)
                print(f"Shipments total: {table_count(cur, 'Shipments')}")

        conn.commit()
        print("\nData generation complete.")


if __name__ == "__main__":
    main()

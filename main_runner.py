"""
main_runner.py

Client API for generating e-commerce database data.
Handles database connection, user input, and calls into data_generators.
"""

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
        table_name (str): Name of the table to count rows in.

    Returns:
        int: Row count from the specified table.
    """
    cur.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cur.fetchone()[0]


def prompt_yes_no(message: str) -> bool:
    """
    Prompt the user for a yes/no response.

    Args:
        message (str): The message/question to display.

    Returns:
        bool: True if user enters 'y', False if user enters 'n'.
    """
    while True:
        resp = input(f"{message} (y/n): ").strip().lower()
        if resp in ("y", "n"):
            return resp == "y"
        print("Invalid input. Please enter 'y' or 'n'.")


def prompt_int(message: str) -> int:
    """
    Prompt the user for a non-negative integer.

    Args:
        message (str): The message/question to display.

    Returns:
        int: The integer entered by the user (>= 0).
    """
    while True:
        try:
            n = int(input(f"{message}: ").strip())
            if n >= 0:
                return n
            print("Please enter a non-negative integer.")
        except ValueError:
            print("Invalid input. Please enter a number.")


def main():
    """
    Main entry point for data generation.

    Connects to the database, prompts the user for each type of data to generate,
    and calls the corresponding generator functions.

    Workflow:
        - Connects to PostgreSQL using environment variables.
        - Prompts for Customers, Products, Orders, Reviews, and Shipments.
        - Skips any section if the user answers 'n' or inputs 0 for the count.
        - Displays row counts after each insert.
        - Commits all inserts once complete.
        - Prints a final summary of all table counts.
    """
    print("Connecting to database...")
    with psycopg2.connect(**db_config) as conn:
        with conn.cursor() as cur:
            # Customers
            if prompt_yes_no("Generate customers?"):
                n = prompt_int("How many customers")
                if n > 0:
                    generate_customers(cur, n)
                    print(f"Customers total: {table_count(cur, 'Customers')}")

            # Products
            if prompt_yes_no("Generate products?"):
                n = prompt_int("How many products")
                if n > 0:
                    generate_products(cur, n)
                    print(f"Products total: {table_count(cur, 'Products')}")

            # Orders
            if prompt_yes_no("Generate orders?"):
                ids = curate_ids(cur)
                n = prompt_int("How many orders")
                if n > 0:
                    generate_orders(cur, ids["customers"], ids["products"], n)
                    print(f"Orders total: {table_count(cur, 'Orders')}")
                    print(f"Order_Items total: {table_count(cur, 'Order_Items')}")

            # Reviews
            if prompt_yes_no("Generate reviews?"):
                ids = curate_ids(cur)
                n = prompt_int("How many reviews")
                if n > 0:
                    generate_reviews(cur, ids["customers"], ids["products"], n)
                    print(f"Reviews total: {table_count(cur, 'Reviews')}")

            # Shipments
            if prompt_yes_no("Generate shipments?"):
                n = prompt_int("How many shipments")
                if n > 0:
                    generate_shipments(cur, n=n)
                    print(f"Shipments total: {table_count(cur, 'Shipments')}")

            # Commit all inserts
            conn.commit()

            # Final summary
            print("\n=== Final Table Counts ===")
            tables = ["Customers", "Products", "Orders", "Order_Items", "Reviews", "Shipments"]
            results = []

            for table in tables:
                try:
                    count = table_count(cur, table)
                    results.append((table, count))
                except Exception:
                    results.append((table, "N/A"))

            # Format as neat table
            col_width = max(len(t[0]) for t in results) + 2
            print(f"{'Table':<{col_width}} | Rows")
            print("-" * (col_width + 8))
            for table, count in results:
                print(f"{table:<{col_width}} | {count}")

        print("\nData generation complete.")


if __name__ == "__main__":
    main()

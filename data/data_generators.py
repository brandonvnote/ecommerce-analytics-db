# File: data_generators.py
# Purpose: Pure data generation and insertion helpers for an e-commerce database.
# All SQL statements (INSERT/SELECT) are centralized in template registries for maintainability.

import random
from faker import Faker
from datetime import timedelta
from typing import List, Tuple, Dict, Optional

fake = Faker()

try:
    from psycopg2.extras import execute_values as _psycopg2_execute_values
except Exception:
    _psycopg2_execute_values = None


# ---------------- SQL Template Registry ----------------

def get_sql_template(key: str) -> str:
    """
    Retrieve an INSERT SQL template by key.

    Args:
        key (str): Identifier for the SQL template.

    Returns:
        str: SQL INSERT template string.
    """
    templates = {
        "customers": "INSERT INTO Customers (first_name, last_name, email, created_at) VALUES %s",
        "products": "INSERT INTO Products (name, category, price, created_at) VALUES %s",
        "orders": "INSERT INTO Orders (customer_id, order_date, status) VALUES %s RETURNING order_id",
        "order_items": "INSERT INTO Order_Items (order_id, product_id, quantity) VALUES %s",
        "reviews": "INSERT INTO Reviews (customer_id, product_id, rating, comment, review_date) VALUES %s",
        "shipments": "INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_method, status) VALUES %s",
    }
    return templates[key]


def get_sql_select(key: str) -> str:
    """
    Retrieve a SELECT SQL template by key.

    Args:
        key (str): Identifier for the SQL template.

    Returns:
        str: SQL SELECT template string.
    """
    templates = {
        "select_customers": "SELECT customer_id FROM Customers",
        "select_products": "SELECT product_id FROM Products",
        "select_orders": "SELECT order_id FROM Orders",
        "select_orders_info": "SELECT order_id, order_date, status FROM Orders",
    }
    return templates[key]


# ---------------- Execution Helpers ----------------

def execute_values(cur, sql: str, argslist: List[tuple]):
    """
    Execute batched INSERT operations.

    Args:
        cur: Database cursor.
        sql (str): SQL INSERT template.
        argslist (List[tuple]): Rows to insert.
    """
    if _psycopg2_execute_values:
        return _psycopg2_execute_values(cur, sql, argslist)
    return cur.executemany(sql, argslist)


# ---------------- ID Curation ----------------

def curate_ids(cur, cache: Optional[Dict[str, List[int]]] = None) -> Dict[str, List[int]]:
    """
    Collect IDs for all entities using SELECT templates.
    Results can be cached and reused to avoid repeated queries.

    Args:
        cur: Database cursor.
        cache (Optional[Dict[str, List[int]]]): Cached IDs to reuse.

    Returns:
        Dict[str, List[int]]: Dictionary of IDs for customers, products, and orders.
    """
    if cache is not None and all(cache.values()):
        return cache

    ids: Dict[str, List[int]] = {"customers": [], "products": [], "orders": []}

    cur.execute(get_sql_select("select_customers"))
    ids["customers"] = [r[0] for r in cur.fetchall()]
    if not ids["customers"]:
        raise RuntimeError("No customers found. Customers must be generated first.")

    cur.execute(get_sql_select("select_products"))
    ids["products"] = [r[0] for r in cur.fetchall()]
    if not ids["products"]:
        raise RuntimeError("No products found. Products must be generated first.")

    cur.execute(get_sql_select("select_orders"))
    ids["orders"] = [r[0] for r in cur.fetchall()]

    return ids


def curate_orders_info(cur, order_ids: Optional[List[int]] = None) -> List[Tuple]:
    """
    Fetch order metadata (order_id, order_date, status).

    Args:
        cur: Database cursor.
        order_ids (Optional[List[int]]): Specific order IDs to filter by.

    Returns:
        List[Tuple]: List of order metadata rows.
    """
    if order_ids:
        placeholders = ",".join(["%s"] * len(order_ids))
        cur.execute(
            f"{get_sql_select('select_orders_info')} WHERE order_id IN ({placeholders})",
            order_ids,
        )
    else:
        cur.execute(get_sql_select("select_orders_info"))
    return cur.fetchall() or []


# ---------------- Data Makers ----------------

def make_customers(n: int) -> list[tuple]:
    """
    Generate fake customer records.

    Args:
        n (int): Number of customers to generate.

    Returns:
        list[tuple]: Generated customer rows.
    """
    customers = []
    for _ in range(n):
        first = fake.first_name()
        last = fake.last_name()
        email = f"{first.lower()}.{last.lower()}@{fake.free_email_domain()}"
        customers.append(
            (
                first,
                last,
                email,
                fake.date_time_between(start_date='-2y', end_date='now')
            )
        )
    return customers


def make_products(n: int) -> list[tuple]:
    """
    Generate fake product records with category-specific price ranges.

    Args:
        n (int): Number of products to generate.

    Returns:
        list[tuple]: Generated product rows.
    """
    categories = {
        "Electronics": (50, 2000),
        "Home": (10, 500),
        "Clothing": (5, 150),
        "Books": (5, 50),
        "Toys": (5, 100),
        "Sports": (10, 300),
        "Beauty": (5, 100)
    }
    products = []
    for _ in range(n):
        category = random.choice(list(categories.keys()))
        price_min, price_max = categories[category]
        name = f"{fake.company()} {fake.word().capitalize()}"
        products.append(
            (
                name,
                category,
                round(random.uniform(price_min, price_max), 2),
                fake.date_time_between(start_date='-2y', end_date='now')
            )
        )
    return products
  

def make_orders_and_items(customer_ids: List[int], product_ids: List[int], n: int):
    """
    Generate orders and a builder for associated order items.

    Args:
        customer_ids (List[int]): Available customer IDs.
        product_ids (List[int]): Available product IDs.
        n (int): Number of orders to generate.

    Returns:
        tuple: (orders, build_items function).
    """
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
    orders = []
    for _ in range(n):
        cid = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choice(statuses)
        orders.append((cid, order_date, status))

    def build_items(order_ids: List[int]):
        """
        Build order item rows for given order IDs.

        Args:
            order_ids (List[int]): List of order IDs.

        Returns:
            list[tuple]: Generated order item rows.
        """
        items = []
        for oid in order_ids:
            max_items = min(4, len(product_ids))
            k = random.randint(1, max_items)
            chosen = random.sample(product_ids, k)
            for pid in chosen:
                items.append((oid, pid, random.randint(1, 5)))
        return items

    return orders, build_items


def make_reviews(customer_ids: List[int], product_ids: List[int], n: int) -> list[tuple]:
    """
    Generate fake product reviews.

    Args:
        customer_ids (List[int]): Available customer IDs.
        product_ids (List[int]): Available product IDs.
        n (int): Number of reviews to generate.

    Returns:
        list[tuple]: Generated review rows.
    """
    adjectives = ["great", "terrible", "excellent", "poor", "decent", "fantastic", "awful", "amazing"]
    reviews = []
    for _ in range(n):
        cid = random.choice(customer_ids)
        pid = random.choice(product_ids)
        rating = random.randint(1, 5)
        comment = f"This product is {random.choice(adjectives)}. {fake.sentence(nb_words=8)}"
        reviews.append(
            (
                cid,
                pid,
                rating,
                comment,
                fake.date_time_between(start_date='-1y', end_date='now')
            )
        )
    return reviews


def make_shipments(orders: List[Tuple]) -> list[tuple]:
    """
    Generate fake shipment records for orders.

    Args:
        orders (List[Tuple]): List of (order_id, order_date, status).

    Returns:
        list[tuple]: Generated shipment rows.
    """
    carriers = ["UPS", "FedEx", "USPS", "DHL"]
    shipments = []
    for order_id, order_date, status in orders:
        if status.lower() in ('shipped', 'delivered'):
            shipped_date = order_date + timedelta(days=random.randint(1, 3))
            delivery_date = shipped_date + timedelta(days=random.randint(1, 7))
            shipments.append(
                (
                    order_id,
                    shipped_date,
                    delivery_date,
                    random.choice(carriers),
                    'delivered' if status.lower() == 'delivered' else 'in_transit'
                )
            )
    return shipments


# ---------------- Insert Helper ----------------

def insert_batch(cur, sql_key: str, rows: list[tuple], return_ids: bool = False):
    """
    Insert rows into the database.

    Args:
        cur: Database cursor.
        sql_key (str): Key for the SQL INSERT template.
        rows (list[tuple]): Rows to insert.
        return_ids (bool): Whether to return generated IDs.

    Returns:
        Optional[List[int]]: Generated IDs if return_ids=True, else None.
    """
    if not rows:
        return [] if return_ids else None

    sql = get_sql_template(sql_key)
    execute_values(cur, sql, rows)
    if return_ids:
        return [r[0] for r in cur.fetchall()]
    return None


# ---------------- Public API (Generators & Inserts) ----------------

def generate_customers(cur, n: int):
    """
    Insert generated customers into the database.

    Args:
        cur: Database cursor.
        n (int): Number of customers to generate.
    """
    insert_batch(cur, "customers", make_customers(n))


def generate_products(cur, n: int):
    """
    Insert generated products into the database.

    Args:
        cur: Database cursor.
        n (int): Number of products to generate.
    """
    insert_batch(cur, "products", make_products(n))


def generate_orders(cur, customer_ids: List[int], product_ids: List[int], n: int) -> List[int]:
    """
    Insert generated orders and order items into the database.

    Args:
        cur: Database cursor.
        customer_ids (List[int]): Existing customer IDs.
        product_ids (List[int]): Existing product IDs.
        n (int): Number of orders to generate.

    Returns:
        List[int]: Generated order IDs.
    """
    if not customer_ids:
        raise RuntimeError("Cannot generate orders: no customers available.")
    if not product_ids:
        raise RuntimeError("Cannot generate orders: no products available.")
    orders, build_items = make_orders_and_items(customer_ids, product_ids, n=n)
    order_ids = insert_batch(cur, "orders", orders, return_ids=True) or []
    if order_ids:
        insert_batch(cur, "order_items", build_items(order_ids))
    return order_ids


def generate_reviews(cur, customer_ids: List[int], product_ids: List[int], n: int):
    """
    Insert generated reviews into the database.

    Args:
        cur: Database cursor.
        customer_ids (List[int]): Existing customer IDs.
        product_ids (List[int]): Existing product IDs.
        n (int): Number of reviews to generate.
    """
    if not customer_ids:
        raise RuntimeError("Cannot generate reviews: no customers available.")
    if not product_ids:
        raise RuntimeError("Cannot generate reviews: no products available.")
    insert_batch(cur, "reviews", make_reviews(customer_ids, product_ids, n=n))


def generate_shipments(cur, orders_info: Optional[List[Tuple]] = None, n: int = 0):
    """
    Insert generated shipments into the database.

    Args:
        cur: Database cursor.
        orders_info (Optional[List[Tuple]]): Existing orders with metadata.
        n (int): Limit of shipments to generate (0 = no limit).
    """
    if orders_info is None:
        orders_info = curate_orders_info(cur)
    if not orders_info:
        raise RuntimeError("Cannot generate shipments: no orders available.")
    shipments = make_shipments(orders_info)
    if n:
        shipments = shipments[:n]
    insert_batch(cur, "shipments", shipments)

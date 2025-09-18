import random
from faker import Faker
from datetime import timedelta, datetime
from typing import Iterable, List, Tuple

fake = Faker()

try:
    from psycopg2.extras import execute_values as _psycopg2_execute_values
except Exception:
    _psycopg2_execute_values = None


def execute_values(cur, sql: str, argslist: Iterable[tuple]):
    if _psycopg2_execute_values:
        return _psycopg2_execute_values(cur, sql, argslist)
    return cur.executemany(sql, argslist)


def chunked_iterable(items: Iterable, size: int = 100):
    chunk = []
    for it in items:
        chunk.append(it)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


# ---------------- SQL Templates ----------------
SQL_TEMPLATES = {
    "customers": "INSERT INTO Customers (first_name, last_name, email, created_at) VALUES %s",
    "products": "INSERT INTO Products (name, category, price, created_at) VALUES %s",
    "orders": "INSERT INTO Orders (customer_id, order_date, status) VALUES %s RETURNING order_id",
    "order_items": "INSERT INTO Order_Items (order_id, product_id, quantity) VALUES %s",
    "reviews": "INSERT INTO Reviews (customer_id, product_id, rating, comment, review_date) VALUES %s",
    "shipments": "INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_method, status) VALUES %s"
}


# ---------------- Data Makers ----------------

def make_customers(n: int = 50) -> list[tuple]:
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


def make_products(n: int = 30) -> list[tuple]:
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


def make_orders_and_items(customer_ids: List[int], product_ids: List[int], n: int = 10):
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'returned']
    orders = []
    for _ in range(n):
        cid = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choice(statuses)
        orders.append((cid, order_date, status))

    def build_items(order_ids: List[int]):
        return [
            (oid, random.choice(product_ids), random.randint(1, 5))
            for oid in order_ids
            for _ in range(random.randint(1, 4))
        ]

    return orders, build_items


def make_reviews(customer_ids: List[int], product_ids: List[int], n: int = 50) -> list[tuple]:
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

def insert_batch(cur, sql_key: str, rows: list[tuple], chunk_size: int = 100, return_ids: bool = False):
    sql = SQL_TEMPLATES[sql_key]
    ids = []
    for chunk in chunked_iterable(rows, size=chunk_size):
        execute_values(cur, sql, chunk)
        if return_ids:
            ids.extend(r[0] for r in cur.fetchall())
    return ids if return_ids else None


# ---------------- Public API ----------------

def generate_customers(cur, n: int = 50):
    insert_batch(cur, "customers", make_customers(n))


def generate_products(cur, n: int = 30):
    insert_batch(cur, "products", make_products(n))


def generate_orders(cur, n: int = 100, num_customers: int = 50):
    try:
        cur.execute("SELECT customer_id FROM Customers")
        rows = cur.fetchall()
        customer_ids = [r[0] for r in rows] if rows else list(range(1, num_customers + 1))
    except Exception:
        customer_ids = list(range(1, num_customers + 1))

    try:
        cur.execute("SELECT product_id FROM Products")
        rows = cur.fetchall()
        product_ids = [r[0] for r in rows] if rows else list(range(1, 11))
    except Exception:
        product_ids = list(range(1, 11))

    orders, build_items = make_orders_and_items(customer_ids, product_ids, n=n)
    order_ids = insert_batch(cur, "orders", orders, return_ids=True)

    if order_ids:
        insert_batch(cur, "order_items", build_items(order_ids))


def generate_reviews(cur, n: int = 100, num_customers: int = 50, num_products: int = 30):
    try:
        cur.execute("SELECT customer_id FROM Customers")
        rows = cur.fetchall()
        customer_ids = [r[0] for r in rows] if rows else list(range(1, num_customers + 1))
    except Exception:
        customer_ids = list(range(1, num_customers + 1))

    try:
        cur.execute("SELECT product_id FROM Products")
        rows = cur.fetchall()
        product_ids = [r[0] for r in rows] if rows else list(range(1, num_products + 1))
    except Exception:
        product_ids = list(range(1, num_products + 1))

    insert_batch(cur, "reviews", make_reviews(customer_ids, product_ids, n=n))


def generate_shipments(cur, n: int = 100):
    cur.execute("SELECT order_id, order_date, status FROM Orders")
    orders = cur.fetchall()
    if not orders:
        return
    shipments = make_shipments(orders)
    if shipments and n:
        shipments = shipments[:n]
    insert_batch(cur, "shipments", shipments)

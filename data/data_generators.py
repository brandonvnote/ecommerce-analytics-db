import random
from faker import Faker
from datetime import timedelta, datetime
from typing import Iterable, List, Tuple, Callable

fake = Faker()

# Try to import execute_values from psycopg2.extras, but provide a safe
# fallback so tests (which patch execute_values) can still work without
# requiring psycopg2 to be installed.
try:
    from psycopg2.extras import execute_values as _psycopg2_execute_values
except Exception:
    _psycopg2_execute_values = None


def execute_values(cur, sql: str, argslist: Iterable[tuple]):
    """Wrapper around psycopg2.extras.execute_values if available,
    otherwise fall back to executemany. Tests patch this symbol.
    """
    if _psycopg2_execute_values:
        return _psycopg2_execute_values(cur, sql, argslist)
    # fallback: simple executemany using the SQL with placeholders
    return cur.executemany(sql, argslist)


def chunked_iterable(items: Iterable, size: int = 100):
    """Yield successive chunks (as lists) from items with the given size."""
    chunk = []
    for it in items:
        chunk.append(it)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def make_customers(n: int = 50) -> List[Tuple[str, str, str]]:
    """Return a list of (first_name, last_name, email). Handles fake.unique exceptions by retrying."""
    customers = []
    for _ in range(n):
        first = fake.first_name()
        last = fake.last_name()
        # fake.unique.email may occasionally raise; handle and retry once
        try:
            email = fake.unique.email()
        except Exception:
            # clear unique state and try once more
            try:
                fake.unique.clear()
            except Exception:
                pass
            email = fake.unique.email()
        customers.append((first, last, email))
    return customers


def make_products(n: int = 30) -> List[Tuple[str, str, float]]:
    categories = ["Electronics", "Home", "Clothing", "Books", "Toys", "Sports", "Beauty"]
    prods = []
    for _ in range(n):
        name = fake.word().capitalize() + " " + fake.word().capitalize()
        cat = random.choice(categories)
        price = round(random.uniform(5.0, 500.0), 2)
        prods.append((name, cat, float(price)))
    return prods


def make_orders_and_items(customer_ids: List[int], product_ids: List[int], n: int = 10) -> Tuple[List[Tuple], Callable[[List[int]], List[Tuple]]]:
    """Return orders (customer_id, order_date, status) and a function to build order items given order_ids."""
    statuses = ['pending', 'shipped', 'delivered', 'cancelled']
    orders = []
    for _ in range(n):
        cid = random.choice(customer_ids)
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choice(statuses)
        orders.append((cid, order_date, status))

    def build_items(order_ids: List[int]) -> List[Tuple[int, int, int]]:
        items = []
        for oid in order_ids:
            # create 1-3 items per order
            for _ in range(random.randint(1, 3)):
                pid = random.choice(product_ids)
                qty = random.randint(1, 3)
                items.append((oid, pid, qty))
        return items

    return orders, build_items


def make_reviews(customer_ids: List[int], product_ids: List[int], n: int = 50) -> List[Tuple[int, int, int, str]]:
    reviews = []
    for _ in range(n):
        cid = random.choice(customer_ids)
        pid = random.choice(product_ids)
        rating = random.randint(1, 5)
        comment = fake.sentence(nb_words=12)
        reviews.append((cid, pid, rating, comment))
    return reviews


def make_shipments(orders: List[Tuple]) -> List[Tuple[int, datetime, datetime, str, str]]:
    """Build shipments for orders whose status is 'shipped' or 'delivered'.
    Each shipment is (order_id, shipped_date, delivery_date, carrier, status_keyword)
    """
    carriers = ["UPS", "FedEx", "USPS", "DHL"]
    shipments = []
    for order in orders:
        # Expect order to be (order_id, order_date, status)
        try:
            order_id, order_date, status = order
        except Exception:
            continue
        if status.lower() in ('shipped', 'delivered'):
            shipped_date = order_date
            delivery_date = shipped_date + timedelta(days=random.randint(1, 7))
            carrier = random.choice(carriers)
            status_kw = 'delivered' if status.lower() == 'delivered' else 'in_transit'
            shipments.append((order_id, shipped_date, delivery_date, carrier, status_kw))
    return shipments


def insert_customer_batch(cur, customers: Iterable[Tuple[str, str, str]], chunk_size: int = 100):
    sql = "INSERT INTO Customers (first_name, last_name, email) VALUES %s"
    for chunk in chunked_iterable(customers, size=chunk_size):
        execute_values(cur, sql, chunk)


def insert_product_batch(cur, products: Iterable[Tuple[str, str, float]], chunk_size: int = 100):
    sql = "INSERT INTO Products (name, category, price) VALUES %s"
    for chunk in chunked_iterable(products, size=chunk_size):
        execute_values(cur, sql, chunk)


def insert_order_batch(cur, orders: Iterable[Tuple], chunk_size: int = 100) -> List[int]:
    sql = "INSERT INTO Orders (customer_id, order_date, status) VALUES %s RETURNING order_id"
    ids = []
    for chunk in chunked_iterable(orders, size=chunk_size):
        execute_values(cur, sql, chunk)
        # caller (tests) may have mocked cur.fetchall to return rows
        rows = cur.fetchall()
        ids.extend([r[0] for r in rows])
    return ids


def insert_order_item_batch(cur, items: Iterable[Tuple[int, int, int]], chunk_size: int = 100):
    sql = "INSERT INTO Order_Items (order_id, product_id, quantity) VALUES %s"
    for chunk in chunked_iterable(items, size=chunk_size):
        execute_values(cur, sql, chunk)


def insert_review_batch(cur, reviews: Iterable[Tuple[int, int, int, str]], chunk_size: int = 100):
    sql = "INSERT INTO Reviews (customer_id, product_id, rating, comment) VALUES %s"
    for chunk in chunked_iterable(reviews, size=chunk_size):
        execute_values(cur, sql, chunk)


def insert_shipment_batch(cur, shipments: Iterable[Tuple], chunk_size: int = 100):
    sql = "INSERT INTO Shipments (order_id, shipped_date, delivery_date, carrier, status) VALUES %s"
    for chunk in chunked_iterable(shipments, size=chunk_size):
        execute_values(cur, sql, chunk)


def generate_customers(cur, n: int = 50):
    """Generate customers and insert them using insert_customer_batch."""
    customers = make_customers(n)
    insert_customer_batch(cur, customers)


def generate_products(cur, n: int = 30):
    """Generate products and insert them using insert_product_batch."""
    products = make_products(n)
    insert_product_batch(cur, products)


def generate_orders(cur, n: int = 100, num_customers: int = 50):
    """Generate orders and insert using insert_order_batch."""
    # build simple order tuples (customer_id, order_date, status)
    customer_ids = list(range(1, num_customers + 1))
    orders, _ = make_orders_and_items(customer_ids, list(range(1, 11)), n=n)
    insert_order_batch(cur, orders)


def generate_reviews(cur, n: int = 100, num_customers: int = 50, num_products: int = 30):
    reviews = make_reviews(list(range(1, num_customers + 1)), list(range(1, num_products + 1)), n=n)
    insert_review_batch(cur, reviews)


def generate_shipments(cur):
    """Query orders and generate shipments for shipped/delivered orders, then insert via insert_shipment_batch.
    If there are no orders returned, do nothing (tests expect insert_shipment_batch not to be called).
    """
    cur.execute("SELECT order_id, order_date, status FROM Orders")
    orders = cur.fetchall()
    if not orders:
        return
    shipments = make_shipments(orders)
    if shipments:
        insert_shipment_batch(cur, shipments)

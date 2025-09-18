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


def insert_customer_batch(cur, customers: Iterable[Tuple[str, str, str]], chunk_size: int = 100, report: dict | None = None):
    sql = "INSERT INTO Customers (first_name, last_name, email) VALUES %s"
    count = 0
    for chunk in chunked_iterable(customers, size=chunk_size):
        execute_values(cur, sql, chunk)
        count += len(chunk)
    if report is not None:
        report.setdefault("customers", 0)
        report["customers"] += count


def insert_product_batch(cur, products: Iterable[Tuple[str, str, float]], chunk_size: int = 100, report: dict | None = None):
    sql = "INSERT INTO Products (name, category, price) VALUES %s"
    count = 0
    for chunk in chunked_iterable(products, size=chunk_size):
        execute_values(cur, sql, chunk)
        count += len(chunk)
    if report is not None:
        report.setdefault("products", 0)
        report["products"] += count


def insert_order_batch(cur, orders: Iterable[Tuple], chunk_size: int = 100, report: dict | None = None) -> List[int]:
    sql = "INSERT INTO Orders (customer_id, order_date, status) VALUES %s RETURNING order_id"
    ids = []
    count = 0
    for chunk in chunked_iterable(orders, size=chunk_size):
        execute_values(cur, sql, chunk)
        # caller (tests) may have mocked cur.fetchall to return rows
        rows = cur.fetchall()
        ids.extend([r[0] for r in rows])
        count += len(chunk)
    if report is not None:
        report.setdefault("orders", 0)
        report["orders"] += count
    return ids


def insert_order_item_batch(cur, items: Iterable[Tuple[int, int, int]], chunk_size: int = 100, report: dict | None = None):
    # Aggregate duplicate (order_id, product_id) pairs by summing quantities
    agg = {}
    for oid, pid, qty in items:
        key = (oid, pid)
        agg[key] = agg.get(key, 0) + qty

    sql = "INSERT INTO Order_Items (order_id, product_id, quantity) VALUES %s"
    count = 0
    # convert aggregated dict into tuples
    deduped = [(oid, pid, qty) for (oid, pid), qty in agg.items()]
    for chunk in chunked_iterable(deduped, size=chunk_size):
        execute_values(cur, sql, chunk)
        count += len(chunk)
    if report is not None:
        report.setdefault("order_items", 0)
        report["order_items"] += count


def insert_review_batch(cur, reviews: Iterable[Tuple[int, int, int, str]], chunk_size: int = 100, report: dict | None = None):
    sql = "INSERT INTO Reviews (customer_id, product_id, rating, comment) VALUES %s"
    count = 0
    for chunk in chunked_iterable(reviews, size=chunk_size):
        execute_values(cur, sql, chunk)
        count += len(chunk)
    if report is not None:
        report.setdefault("reviews", 0)
        report["reviews"] += count


def insert_shipment_batch(cur, shipments: Iterable[Tuple], chunk_size: int = 100, report: dict | None = None):
    # use shipping_method to match the schema's column name
    sql = "INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_method, status) VALUES %s"
    count = 0
    for chunk in chunked_iterable(shipments, size=chunk_size):
        execute_values(cur, sql, chunk)
        count += len(chunk)
    if report is not None:
        report.setdefault("shipments", 0)
        report["shipments"] += count


def generate_customers(cur, n: int = 50, report: dict | None = None, chunk_size: int = 100):
    """Generate customers and insert them using insert_customer_batch."""
    customers = make_customers(n)
    insert_customer_batch(cur, customers, report=report, chunk_size=chunk_size)


def generate_products(cur, n: int = 30, report: dict | None = None, chunk_size: int = 100):
    """Generate products and insert them using insert_product_batch."""
    products = make_products(n)
    insert_product_batch(cur, products, report=report, chunk_size=chunk_size)


def generate_orders(cur, n: int = 100, num_customers: int = 50, report: dict | None = None, chunk_size: int = 100):
    """Generate orders and insert using insert_order_batch."""
    # Prefer using actual customer IDs from the database if a cursor is available
    customer_ids = None
    try:
        # Attempt to read existing customer ids from the Customers table
        cur.execute("SELECT customer_id FROM Customers")
        rows = cur.fetchall()
        if rows:
            customer_ids = [r[0] for r in rows]
    except Exception:
        # If querying fails (no DB or table), fall back to the numeric range
        customer_ids = None

    if not customer_ids:
        customer_ids = list(range(1, num_customers + 1))

    # Attempt to use actual product ids from the DB to ensure FK validity for order items
    product_ids = None
    try:
        cur.execute("SELECT product_id FROM Products")
        prow = cur.fetchall()
        if prow:
            product_ids = [r[0] for r in prow]
    except Exception:
        product_ids = None

    if not product_ids:
        product_ids = list(range(1, 11))

    orders, build_items = make_orders_and_items(customer_ids, product_ids, n=n)
    # insert orders and get back created order ids (insert_order_batch uses cur.fetchall())
    order_ids = insert_order_batch(cur, orders, report=report, chunk_size=chunk_size)

    # build and insert order items tied to the inserted order ids
    if order_ids:
        items = build_items(order_ids)
        if items:
            insert_order_item_batch(cur, items, report=report, chunk_size=chunk_size)


def generate_reviews(cur, n: int = 100, num_customers: int = 50, num_products: int = 30, report: dict | None = None, chunk_size: int = 100):
    # Try to use actual customer and product ids from the DB when available
    customer_ids = None
    product_ids = None
    try:
        cur.execute("SELECT customer_id FROM Customers")
        rows = cur.fetchall()
        if rows:
            customer_ids = [r[0] for r in rows]
    except Exception:
        customer_ids = None

    try:
        cur.execute("SELECT product_id FROM Products")
        rows = cur.fetchall()
        if rows:
            product_ids = [r[0] for r in rows]
    except Exception:
        product_ids = None

    if not customer_ids:
        customer_ids = list(range(1, num_customers + 1))
    if not product_ids:
        product_ids = list(range(1, num_products + 1))

    reviews = make_reviews(customer_ids, product_ids, n=n)
    insert_review_batch(cur, reviews, report=report, chunk_size=chunk_size)


def generate_shipments(cur, report: dict | None = None, n: int = 100, chunk_size: int = 100):
    """Query orders and generate shipments for shipped/delivered orders, then insert via insert_shipment_batch.
    If there are no orders returned, do nothing (tests expect insert_shipment_batch not to be called).
    """
    cur.execute("SELECT order_id, order_date, status FROM Orders")
    orders = cur.fetchall()
    if not orders:
        return
    shipments = make_shipments(orders)
    if shipments:
        # optionally limit number of shipments generated when 'n' provided
        if n is not None:
            shipments = shipments[:n]
        insert_shipment_batch(cur, shipments, report=report, chunk_size=chunk_size)

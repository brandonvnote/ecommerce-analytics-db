import types
import sys
import os
import pytest
from datetime import datetime

# Ensure project root is importable when pytest runs
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data import data_generators as dg


def test_make_customers_basic():
    rows = dg.make_customers(5)
    assert len(rows) == 5
    for r in rows:
        assert len(r) == 4
        first, last, email, created = r
        assert "@" in email
        assert isinstance(created, datetime)


def test_make_products_basic():
    rows = dg.make_products(6)
    assert len(rows) == 6
    for name, category, price, created in rows:
        assert isinstance(name, str) and name
        assert category in (
            "Electronics",
            "Home",
            "Clothing",
            "Books",
            "Toys",
            "Sports",
            "Beauty",
        )
        assert isinstance(price, float) or isinstance(price, int)
        assert isinstance(created, datetime)



def test_make_orders_and_items_build():
    customer_ids = [1, 2, 3]
    product_ids = [10, 20]
    orders, build_items = dg.make_orders_and_items(customer_ids, product_ids, n=4)
    assert len(orders) == 4
    # simulate returned order ids
    fake_order_ids = [101, 102, 103, 104]
    items = build_items(fake_order_ids)
    assert items
    for oid, pid, qty in items:
        assert oid in fake_order_ids
        assert pid in product_ids
        assert 1 <= qty <= 5


def test_make_reviews_basic():
    customer_ids = [1, 2]
    product_ids = [5, 6]
    reviews = dg.make_reviews(customer_ids, product_ids, n=8)
    assert len(reviews) == 8
    for cid, pid, rating, comment, review_date in reviews:
        assert cid in customer_ids
        assert pid in product_ids
        assert 1 <= rating <= 5
        assert isinstance(comment, str)
        assert isinstance(review_date, datetime)


def test_make_shipments_filters_and_dates():
    now = datetime.now()
    orders = [
        (1, now, "pending"),
        (2, now, "shipped"),
        (3, now, "delivered"),
        (4, now, "cancelled"),
    ]
    shipments = dg.make_shipments(orders)
    # Only 'shipped' and 'delivered' should produce shipments
    assert len(shipments) == 2
    for order_id, shipped_date, delivery_date, carrier, status in shipments:
        assert order_id in (2, 3)
        assert shipped_date >= now
        assert delivery_date >= shipped_date
        assert carrier in ("UPS", "FedEx", "USPS", "DHL")


def test_execute_values_fallback_and_insert_batch_returns_ids():
    # Force fallback to executemany path
    original = dg._psycopg2_execute_values
    dg._psycopg2_execute_values = None

    class FakeCursor:
        def __init__(self):
            self.executed = []

        def executemany(self, sql, argslist):
            # record call
            self.executed.append((sql, list(argslist)))

        def fetchall(self):
            # Simulate RETURNING order_id rows
            return [(201,), (202,)]

    cur = FakeCursor()
    rows = [(1, "2025-01-01", "pending"), (2, "2025-01-02", "shipped")]
    ids = dg.insert_batch(cur, "orders", rows, return_ids=True)
    assert ids == [201, 202]
    assert cur.executed

    # restore
    dg._psycopg2_execute_values = original

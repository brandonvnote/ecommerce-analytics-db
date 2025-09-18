import sys
import os
import pytest

# Ensure project root is importable
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from data import data_generators as dg


def test_execute_values_uses_psycopg2_execute_values(monkeypatch):
    called = {}

    def fake_execvals(cur, sql, argslist):
        called['args'] = (cur, sql, list(argslist))
        return 'ok'

    original = dg._psycopg2_execute_values
    monkeypatch.setattr(dg, '_psycopg2_execute_values', fake_execvals)

    class FakeCur:
        pass

    cur = FakeCur()
    rows = [(1, 2), (3, 4)]
    res = dg.execute_values(cur, 'SQL', rows)
    assert called['args'][0] is cur
    assert called['args'][1] == 'SQL'
    assert called['args'][2] == rows

    # restore
    monkeypatch.setattr(dg, '_psycopg2_execute_values', original)


def test_insert_batch_chunking_calls_execute_values_multiple_times(monkeypatch):
    calls = []

    def fake_execute_values(cur, sql, argslist):
        calls.append(list(argslist))

    monkeypatch.setattr(dg, 'execute_values', fake_execute_values)

    # make 10 small tuples; chunk_size 3 -> should call 4 times
    rows = [(i,) for i in range(10)]
    class FakeCur:
        def fetchall(self):
            return []

    dg.insert_batch(FakeCur(), 'customers', rows, chunk_size=3)
    assert len(calls) == 4
    # ensure the concatenation of calls equals original rows
    flattened = [item for sub in calls for item in sub]
    assert flattened == rows


def test_generate_orders_uses_default_ids_when_select_fails(monkeypatch):
    # cur.execute will raise to force default ids
    class BadCur:
        def execute(self, sql):
            raise Exception('db error')

        def fetchall(self):
            return []

    made = {}

    def fake_make_orders_and_items(customer_ids, product_ids, n=0):
        made['customer_ids'] = customer_ids
        made['product_ids'] = product_ids
        return ([], lambda x: [])

    # simulate insert_batch returning order ids for orders
    def fake_insert_batch(cur, sql_key, rows, chunk_size=100, return_ids=False):
        if sql_key == 'orders' and return_ids:
            return [101, 102]
        return None

    monkeypatch.setattr(dg, 'make_orders_and_items', fake_make_orders_and_items)
    monkeypatch.setattr(dg, 'insert_batch', fake_insert_batch)

    dg.generate_orders(BadCur(), n=2, num_customers=5)
    assert made['customer_ids'] == list(range(1, 6))


def test_generate_reviews_uses_default_ids_when_select_fails(monkeypatch):
    class BadCur:
        def execute(self, sql):
            raise Exception('db error')

        def fetchall(self):
            return []

    made = {}

    def fake_make_reviews(customer_ids, product_ids, n=0):
        made['customer_ids'] = customer_ids
        made['product_ids'] = product_ids
        return []

    monkeypatch.setattr(dg, 'make_reviews', fake_make_reviews)
    monkeypatch.setattr(dg, 'insert_batch', lambda *a, **k: None)

    dg.generate_reviews(BadCur(), n=3, num_customers=4, num_products=6)
    assert made['customer_ids'] == list(range(1, 5))
    assert made['product_ids'] == list(range(1, 7))


def test_generate_shipments_slices_and_inserts(monkeypatch):
    # create orders where some are shipped/delivered
    now = dg.fake.date_time_between(start_date='-1y', end_date='now')
    orders = [
        (1, now, 'pending'),
        (2, now, 'shipped'),
        (3, now, 'delivered'),
        (4, now, 'shipped'),
    ]

    class Cur:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, sql):
            pass

        def fetchall(self):
            return self._rows

    recorded = {}

    def fake_insert_batch(cur, sql_key, rows, chunk_size=100, return_ids=False):
        recorded['sql_key'] = sql_key
        recorded['rows'] = rows

    monkeypatch.setattr(dg, 'insert_batch', fake_insert_batch)

    dg.generate_shipments(Cur(orders), n=2)
    # should only insert shipments and be limited to n=2
    assert recorded['sql_key'] == 'shipments'
    assert len(recorded['rows']) <= 2

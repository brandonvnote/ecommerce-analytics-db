import sys
import os
import types
import builtins
import pytest

# ensure project root importable
proj_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(proj_root)

from data import data_generators as dg
import main_runner


def test_curate_ids_uses_cache_directly():
    cache = {"customers": [1], "products": [2], "orders": [3]}
    class Cur:
        def execute(self, sql, params=None):
            raise AssertionError("should not be called when cache provided")
    cur = Cur()
    out = dg.curate_ids(cur, cache=cache)
    assert out is cache


def test_curate_ids_raises_when_missing_customers():
    class Cur:
        def __init__(self):
            self._last = None
        def execute(self, sql, params=None):
            self._last = sql
        def fetchall(self):
            # when called for customers return empty
            if 'CUSTOMER' in (self._last or '').upper():
                return []
            return [(1,)]

    with pytest.raises(RuntimeError):
        dg.curate_ids(Cur())


def test_curate_orders_info_with_and_without_filter():
    recorded = {}

    class Cur:
        def execute(self, sql, params=None):
            recorded['sql'] = sql
            recorded['params'] = params
        def fetchall(self):
            return [(11, '2025-01-01', 'shipped')]

    cur = Cur()
    # without ids
    out = dg.curate_orders_info(cur)
    assert out and out[0][0] == 11

    # with ids
    recorded.clear()
    out2 = dg.curate_orders_info(cur, order_ids=[101, 102])
    assert 'WHERE order_id IN' in recorded['sql']
    assert recorded['params'] == [101, 102]


def test_insert_batch_empty_rows_returns_none_or_empty():
    class Cur:
        def fetchall(self):
            return []

    cur = Cur()
    assert dg.insert_batch(cur, 'customers', []) is None
    assert dg.insert_batch(cur, 'customers', [], return_ids=True) == []


def test_generate_orders_inserts_items_when_order_ids_returned(monkeypatch):
    made = {}

    def fake_make_orders_and_items(customer_ids, product_ids, n=0):
        made['called_make'] = True
        # return fake orders and build_items function
        return ([(1, '2025-01-01', 'pending')], lambda oids: [(oids[0], 10, 2)])

    calls = []

    def fake_insert_batch(cur, sql_key, rows, return_ids=False):
        calls.append((sql_key, list(rows)))
        if sql_key == 'orders' and return_ids:
            return [501]
        return None

    monkeypatch.setattr(dg, 'make_orders_and_items', fake_make_orders_and_items)
    monkeypatch.setattr(dg, 'insert_batch', fake_insert_batch)

    # call with valid ids
    res = dg.generate_orders(cur=None, customer_ids=[1], product_ids=[10], n=1)
    # should have attempted to insert orders then order_items
    assert any(k == 'orders' for k, _ in calls)
    assert any(k == 'order_items' for k, _ in calls)


def test_generate_shipments_raises_when_no_orders(monkeypatch):
    # simulate curate_orders_info returning empty
    monkeypatch.setattr(dg, 'curate_orders_info', lambda cur=None: [])
    with pytest.raises(RuntimeError):
        dg.generate_shipments(cur=None)


def test_main_runner_interactive_generation(monkeypatch):
    # Fake connection and cursor like previous tests but track generator calls
    class FakeCursor:
        def __init__(self):
            self._last = None
            self.counts = {"Customers": 0, "Products": 0, "Orders": 0, "Order_Items": 0, "Reviews": 0, "Shipments": 0}
        def execute(self, sql):
            self._last = sql
        def fetchone(self):
            tbl = self._last.split()[-1]
            return (self.counts.get(tbl, 0),)
        def fetchall(self):
            return []
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()
            self.committed = False
        def cursor(self):
            return self.cur
        def commit(self):
            self.committed = True
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConn()
    monkeypatch.setattr(main_runner.psycopg2, 'connect', lambda **kwargs: fake_conn)

    called = {}
    monkeypatch.setattr(main_runner, 'generate_customers', lambda cur, n: called.setdefault('customers', n))
    monkeypatch.setattr(main_runner, 'generate_products', lambda cur, n: called.setdefault('products', n))
    monkeypatch.setattr(main_runner, 'generate_orders', lambda *a, **k: called.setdefault('orders', True))
    monkeypatch.setattr(main_runner, 'generate_reviews', lambda *a, **k: called.setdefault('reviews', True))
    monkeypatch.setattr(main_runner, 'generate_shipments', lambda *a, **k: called.setdefault('shipments', True))

    # inputs: generate customers? y -> how many? 2; products? y -> 3; orders? n; reviews? n; shipments? n
    inputs = iter(['y', '2', 'y', '3', 'n', 'n', 'n'])
    monkeypatch.setattr('builtins.input', lambda *args, **kwargs: next(inputs))

    # run main
    main_runner.main()

    # verify our patched generators were invoked with expected args
    assert called.get('customers') == '2' or called.get('customers') == 2
    assert called.get('products') == '3' or called.get('products') == 3


def test_prompt_yes_no_invalid_then_valid(monkeypatch, capsys):
    # first invalid input then valid 'Y'
    inputs = iter(['invalid', 'Y'])
    monkeypatch.setattr('builtins.input', lambda *a, **k: next(inputs))
    res = main_runner.prompt_yes_no('Proceed?')
    # should return True for 'Y' and print the validation message once
    captured = capsys.readouterr()
    assert "Please enter 'y' or 'n'." in captured.out
    assert res is True


def test_main_runner_orders_branch(monkeypatch):
    # create fake conn/cursor that returns counts
    class FakeCursor:
        def __init__(self):
            self._last = None
            self.counts = {"Customers": 0, "Products": 0, "Orders": 7, "Order_Items": 13, "Reviews": 0, "Shipments": 0}
        def execute(self, sql):
            self._last = sql
        def fetchone(self):
            tbl = self._last.split()[-1]
            return (self.counts.get(tbl, 0),)
        def fetchall(self):
            return []
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    class FakeConn:
        def __init__(self):
            self.cur = FakeCursor()
            self.committed = False
        def cursor(self):
            return self.cur
        def commit(self):
            self.committed = True
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    fake_conn = FakeConn()
    monkeypatch.setattr(main_runner.psycopg2, 'connect', lambda **kwargs: fake_conn)

    # Make sure only orders path is taken
    monkeypatch.setattr(main_runner, 'generate_customers', lambda *a, **k: None)
    monkeypatch.setattr(main_runner, 'generate_products', lambda *a, **k: None)
    called = {}
    monkeypatch.setattr(main_runner, 'generate_orders', lambda *a, **k: called.setdefault('orders', True))
    monkeypatch.setattr(main_runner, 'generate_reviews', lambda *a, **k: None)
    monkeypatch.setattr(main_runner, 'generate_shipments', lambda *a, **k: None)

    # curate_ids should return usable ids
    monkeypatch.setattr(main_runner, 'curate_ids', lambda cur: {'customers': [1], 'products': [2], 'orders': [3]})

    # Inputs: customers? n, products? n, orders? y, how many? 2, reviews? n, shipments? n
    inputs = iter(['n', 'n', 'y', '2', 'n', 'n'])
    monkeypatch.setattr('builtins.input', lambda *args, **kwargs: next(inputs))

    main_runner.main()

    assert called.get('orders') is True
    assert fake_conn.committed is True

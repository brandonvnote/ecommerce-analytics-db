import sys
import os
import types
import builtins
import pytest

# Ensure project root and data package dir are importable
proj_root = os.path.dirname(os.path.dirname(__file__))
sys.path.append(proj_root)
sys.path.append(os.path.join(proj_root, "data"))

import main_runner


class FakeCursor:
    def __init__(self):
        self.queries = []
        # simple in-memory table counts
        self.counts = {"Customers": 3, "Products": 4, "Orders": 5, "Order_Items": 6, "Reviews": 7, "Shipments": 2}

    def execute(self, sql):
        self.queries.append(sql)
        # emulate SELECT COUNT(*) FROM {table}
        if sql.strip().upper().startswith("SELECT COUNT(*) FROM"):
            self._last = sql

    def fetchone(self):
        # parse table name
        tbl = self._last.split()[-1]
        return (self.counts.get(tbl, 0),)

    def fetchall(self):
        return []

    # support context manager protocol used by `with conn.cursor() as cur:`
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


def test_table_count_and_main_runs(monkeypatch):
    fake_conn = FakeConn()

    def fake_connect(**kwargs):
        return fake_conn

    # Patch psycopg2.connect used by main_runner
    monkeypatch.setattr(main_runner.psycopg2, "connect", fake_connect)

    # Patch input so prompts return 'n' and main() doesn't block
    monkeypatch.setattr('builtins.input', lambda *a, **k: 'n')

    # Patch generator functions so they don't try to use the DB
    for name in ("generate_customers", "generate_products", "generate_orders", "generate_reviews", "generate_shipments"):
        monkeypatch.setattr(main_runner, name, lambda *a, **k: None)

    # Run main which should use our fake connection and not raise
    main_runner.main()

    # Verify table_count works
    cur = fake_conn.cur
    assert main_runner.table_count(cur, "Customers") == 3
    assert main_runner.table_count(cur, "NonExisting") == 0

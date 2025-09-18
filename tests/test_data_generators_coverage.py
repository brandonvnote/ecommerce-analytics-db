import importlib
from unittest.mock import MagicMock, patch
import data.data_generators as dg


def test_execute_values_uses_fallback_when_psycopg2_missing():
    mock_cur = MagicMock()
    # force fallback path
    original = dg._psycopg2_execute_values
    dg._psycopg2_execute_values = None
    try:
        args = [(1, 2), (3, 4)]
        dg.execute_values(mock_cur, "SQL", args)
        mock_cur.executemany.assert_called_once_with("SQL", args)
    finally:
        dg._psycopg2_execute_values = original


def test_execute_values_uses_psycopg2_when_available():
    mock_cur = MagicMock()
    mock_exec_values = MagicMock()
    original = dg._psycopg2_execute_values
    dg._psycopg2_execute_values = mock_exec_values
    try:
        args = [(1,)]
        dg.execute_values(mock_cur, "SQL2", args)
        mock_exec_values.assert_called_once_with(mock_cur, "SQL2", args)
    finally:
        dg._psycopg2_execute_values = original


def test_make_customers_handles_clear_raising(monkeypatch):
    # Simulate fake.unique.email raising once, then returning a value.
    # Also simulate fake.unique.clear raising so we hit the inner except path.
    emails = [Exception("boom"), 'ok@example.com']
    def email_side_effect():
        val = emails.pop(0)
        if isinstance(val, Exception):
            raise val
        return val

    monkeypatch.setattr(dg.fake.unique, 'email', email_side_effect)
    monkeypatch.setattr(dg.fake.unique, 'clear', lambda: (_ for _ in ()).throw(Exception('clearfail')))

    customers = dg.make_customers(1)
    assert len(customers) == 1
    assert customers[0][2] == 'ok@example.com'


def test_make_shipments_skips_bad_tuples():
    # include a bad tuple to trigger the except and continue
    now = dg.fake.date_time_between(start_date='-1d', end_date='now')
    orders = [ (1, now, 'shipped'), (2,), ('bad',) ]
    shipments = dg.make_shipments(orders)
    # should include only the valid shipped order
    assert any(s[0] == 1 for s in shipments)


def test_chunked_iterable_empty():
    chunks = list(dg.chunked_iterable([], size=3))
    assert chunks == []

import pytest
from unittest.mock import MagicMock, patch
from data import data_generators as dg


def test_generate_customers_inserts():
    mock_cur = MagicMock()
    # patch the actual import path used by the tests
    with patch("data.data_generators.insert_customer_batch") as mock_insert:
        dg.generate_customers(mock_cur, n=3)

        mock_insert.assert_called_once()
        args, _ = mock_insert.call_args
        customers = args[1]
        assert len(customers) == 3
        assert all(len(c) == 3 for c in customers)  # first, last, email


def test_generate_products_inserts():
    mock_cur = MagicMock()
    with patch("data.data_generators.insert_product_batch") as mock_insert:
        dg.generate_products(mock_cur, n=4)

        mock_insert.assert_called_once()
        args, _ = mock_insert.call_args
        products = args[1]
        assert len(products) == 4
        assert all(len(p) == 3 for p in products)  # name, category, price

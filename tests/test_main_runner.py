import unittest
from unittest.mock import MagicMock, patch
import main_runner


class TestMainRunner(unittest.TestCase):

    @patch("main_runner.psycopg2.connect")
    @patch("main_runner.generate_customers")
    @patch("main_runner.generate_products")
    @patch("main_runner.generate_orders")
    @patch("main_runner.generate_reviews")
    @patch("main_runner.generate_shipments")
    def test_main_calls_all_generators(
        self,
        mock_shipments,
        mock_reviews,
        mock_orders,
        mock_products,
        mock_customers,
        mock_connect,
    ):
        # Mock DB connection + cursor context managers
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        mock_connect.return_value.__enter__.return_value = mock_conn
        mock_conn.cursor.return_value.__enter__.return_value = mock_cur

        # Run the main() function
        main_runner.main()

        # Verify psycopg2.connect called
        mock_connect.assert_called_once()

        # Verify all generators called once
        mock_customers.assert_called_once()
        mock_products.assert_called_once()
        mock_orders.assert_called_once()
        mock_reviews.assert_called_once()
        mock_shipments.assert_called_once()


if __name__ == "__main__":
    unittest.main()

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime
from data import data_generators as dg


class TestMakeHelpersMore(unittest.TestCase):

    def test_chunked_iterable_behavior(self):
        items = list(range(7))
        chunks = list(dg.chunked_iterable(items, size=3))
        self.assertEqual(chunks, [[0, 1, 2], [3, 4, 5], [6]])

    def test_make_products_shape(self):
        prods = dg.make_products(5)
        self.assertEqual(len(prods), 5)
        for name, cat, price in prods:
            self.assertIsInstance(name, str)
            self.assertIn(cat, ["Electronics", "Home", "Clothing", "Books", "Toys", "Sports", "Beauty"])
            self.assertIsInstance(price, float)

    def test_make_orders_and_build_items(self):
        customer_ids = [1, 2]
        product_ids = [10, 11, 12]
        orders, build_items = dg.make_orders_and_items(customer_ids, product_ids, n=3)
        self.assertEqual(len(orders), 3)
        for order in orders:
            self.assertEqual(len(order), 3)
        order_ids = [100, 101, 102]
        items = build_items(order_ids)
        self.assertGreaterEqual(len(items), len(order_ids))
        for order_id, product_id, qty in items:
            self.assertIn(order_id, order_ids)
            self.assertIn(product_id, product_ids)
            self.assertTrue(1 <= qty <= 3)

    def test_make_reviews(self):
        cids = [1, 2]
        pids = [10]
        reviews = dg.make_reviews(cids, pids, n=4)
        self.assertEqual(len(reviews), 4)
        for cid, pid, rating, comment in reviews:
            self.assertIn(cid, cids)
            self.assertIn(pid, pids)
            self.assertTrue(1 <= rating <= 5)
            self.assertIsInstance(comment, str)

    def test_make_shipments_filters_and_fields(self):
        now = datetime.now()
        orders = [
            (1, now, 'pending'),
            (2, now, 'shipped'),
            (3, now, 'delivered')
        ]
        shipments = dg.make_shipments(orders)
        # two shipments expected (shipped and delivered)
        self.assertEqual(len(shipments), 2)
        for ship in shipments:
            self.assertEqual(len(ship), 5)
            self.assertIn(ship[3], ["UPS", "FedEx", "USPS", "DHL"])
            self.assertIn(ship[4], ["delivered", "in_transit"])

    def test_insert_helpers_call_execute_values(self):
        mock_cur = MagicMock()
        items = [(1, 2, 3, 4, 5)] * 5
        with patch('data.data_generators.execute_values') as mock_exec, patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [items[:2], items[2:]]
            dg.insert_product_batch(mock_cur, items)
            self.assertEqual(mock_exec.call_count, 2)
            sql = mock_exec.call_args_list[0][0][1]
            self.assertIn('INSERT INTO Products', sql)

        order_items = [(1, 2, 3)] * 4
        with patch('data.data_generators.execute_values') as mock_exec, patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [order_items]
            dg.insert_order_item_batch(mock_cur, order_items)
            mock_exec.assert_called_once()
            self.assertIn('Order_Items', mock_exec.call_args[0][1])

        reviews = [(1, 2, 3, 'c')] * 3
        with patch('data.data_generators.execute_values') as mock_exec, patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [reviews]
            dg.insert_review_batch(mock_cur, reviews)
            mock_exec.assert_called_once()
            self.assertIn('Reviews', mock_exec.call_args[0][1])

        now = datetime.now()
        shipments = [(1, now, now, 'UPS', 'delivered')]
        with patch('data.data_generators.execute_values') as mock_exec, patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [shipments]
            dg.insert_shipment_batch(mock_cur, shipments)
            mock_exec.assert_called_once()
            self.assertIn('Shipments', mock_exec.call_args[0][1])

    def test_make_customers_unique_exception_path(self):
        with patch.object(dg.fake.unique, 'email', side_effect=[Exception('boom'), 'ok@example.com']):
            customers = dg.make_customers(1)
            self.assertEqual(len(customers), 1)
            self.assertEqual(customers[0][2], 'ok@example.com')


if __name__ == '__main__':
    unittest.main()

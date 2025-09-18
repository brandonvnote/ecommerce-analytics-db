import unittest
from unittest.mock import MagicMock, patch
from data import data_generators as dg


class TestDataGeneratorsExtra(unittest.TestCase):

    def test_chunked_iterable(self):
        items = [1, 2, 3, 4, 5]
        chunks = list(dg.chunked_iterable(items, size=2))
        self.assertEqual(chunks, [[1, 2], [3, 4], [5]])

    def test_insert_customer_batch_calls_execute_values(self):
        mock_cur = MagicMock()
        customers = [(f'fn{i}', f'ln{i}', f'user{i}@ex.com') for i in range(5)]
        with patch('data.data_generators.execute_values') as mock_exec, \
             patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [customers[:2], customers[2:]]
            dg.insert_customer_batch(mock_cur, customers)
            self.assertEqual(mock_exec.call_count, 2)
            # check SQL contains the table name
            called_sql = mock_exec.call_args_list[0][0][1]
            self.assertIn('INSERT INTO Customers', called_sql)

    def test_insert_order_batch_returns_ids(self):
        mock_cur = MagicMock()
        orders = [(1, '2020-01-01', 'pending')]
        with patch('data.data_generators.execute_values') as mock_exec, \
             patch('data.data_generators.chunked_iterable') as mock_chunk:
            mock_chunk.return_value = [orders]
            # Simulate the DB returning two order ids for the chunk
            mock_cur.fetchall.side_effect = [[(10,), (20,)]]
            ids = dg.insert_order_batch(mock_cur, orders)
            self.assertEqual(ids, [10, 20])

    def test_generate_customers_unique_exception_clears(self):
        mock_cur = MagicMock()
        # Force fake.unique.email to raise once, then return a valid email
        with patch.object(dg.fake.unique, 'email', side_effect=[Exception('boom'), 'ok@example.com']), \
             patch('data.data_generators.insert_customer_batch') as mock_insert:
            dg.generate_customers(mock_cur, n=1)
            mock_insert.assert_called_once()
            customers = mock_insert.call_args[0][1]
            self.assertEqual(len(customers), 1)
            self.assertEqual(customers[0][2], 'ok@example.com')

    def test_generate_shipments_no_shipments(self):
        mock_cur = MagicMock()
        mock_cur.fetchall.return_value = []
        with patch('data.data_generators.insert_shipment_batch') as mock_ship:
            dg.generate_shipments(mock_cur)
            mock_ship.assert_not_called()


if __name__ == '__main__':
    unittest.main()

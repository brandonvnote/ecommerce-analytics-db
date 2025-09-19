# ---------------- SQL Template Registry ----------------

def get_sql_template(key: str) -> str:
    """
    Retrieve an INSERT SQL template by key.

    Args:
        key (str): Identifier for the SQL template.

    Returns:
        str: SQL INSERT template string.
    """
    templates = {
        "customers": "INSERT INTO Customers (first_name, last_name, email, created_at) VALUES %s",
        "products": "INSERT INTO Products (name, category, price, created_at) VALUES %s",
        "orders": "INSERT INTO Orders (customer_id, order_date, status) VALUES %s RETURNING order_id",
        "order_items": "INSERT INTO Order_Items (order_id, product_id, quantity) VALUES %s",
        "reviews": "INSERT INTO Reviews (customer_id, product_id, rating, comment, review_date) VALUES %s",
        "shipments": "INSERT INTO Shipments (order_id, shipped_date, delivery_date, shipping_method, status) VALUES %s",
    }
    return templates[key]


def get_sql_select(key: str) -> str:
    """
    Retrieve a SELECT SQL template by key.

    Args:
        key (str): Identifier for the SQL template.

    Returns:
        str: SQL SELECT template string.
    """
    templates = {
        "select_customers": "SELECT customer_id FROM Customers",
        "select_products": "SELECT product_id FROM Products",
        "select_orders": "SELECT order_id FROM Orders",
        "select_orders_info": "SELECT order_id, order_date, status FROM Orders",
    }
    return templates[key]
/* ============================================================
   📈 E-COMMERCE ANALYTICS & PERFORMANCE QUERIES
   Schema: Customers, Products, Orders, Order_Items, Reviews
   ============================================================ */

/* ================================
   PERFORMANCE INDEXES
   -------------------------------
   These indexes improve join speed and GROUP BY queries.
   Run them once after schema creation.
   ================================ */

-- Speed up Orders → Customers join
CREATE INDEX IF NOT EXISTS idx_orders_customer_id
    ON Orders(customer_id);

-- Speed up Order_Items → Orders join
CREATE INDEX IF NOT EXISTS idx_order_items_order_id
    ON Order_Items(order_id);

-- Speed up Order_Items → Products join
CREATE INDEX IF NOT EXISTS idx_order_items_product_id
    ON Order_Items(product_id);

-- Helps with GROUP BY category in revenue queries
CREATE INDEX IF NOT EXISTS idx_products_category
    ON Products(category);


/* ================================
   ANALYTICAL QUERIES
   ================================ */

-- 1. Monthly revenue trend
-- Shows how much revenue was generated per month.
SELECT 
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(oi.quantity * p.price) AS total_revenue
FROM Orders o
JOIN Order_Items oi ON o.order_id = oi.order_id
JOIN Products p ON oi.product_id = p.product_id
GROUP BY month
ORDER BY month;


-- 2. Top 10 customers by total spend
-- Finds your most valuable customers.
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(oi.quantity * p.price) AS total_spent
FROM Customers c
JOIN Orders o ON c.customer_id = o.customer_id
JOIN Order_Items oi ON o.order_id = oi.order_id
JOIN Products p ON oi.product_id = p.product_id
GROUP BY c.customer_id, customer_name
ORDER BY total_spent DESC
LIMIT 10;


-- 3. Average items per order
-- Measures how many products customers buy per order.
SELECT 
    AVG(item_count) AS avg_items_per_order
FROM (
    SELECT o.order_id, SUM(oi.quantity) AS item_count
    FROM Orders o
    JOIN Order_Items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id
) sub;


-- 4. Revenue by product category
-- Breaks down revenue contribution per category.
SELECT 
    p.category,
    SUM(oi.quantity * p.price) AS category_revenue
FROM Products p
JOIN Order_Items oi ON p.product_id = oi.product_id
JOIN Orders o ON oi.order_id = o.order_id
GROUP BY p.category
ORDER BY category_revenue DESC;


-- 5. Best-selling products
-- Which products have sold the most units.
SELECT 
    p.product_id,
    p.name,
    SUM(oi.quantity) AS total_units_sold
FROM Products p
JOIN Order_Items oi ON p.product_id = oi.product_id
GROUP BY p.product_id, p.name
ORDER BY total_units_sold DESC
LIMIT 10;


-- 6. Average order value (AOV)
-- Total revenue ÷ number of orders.
SELECT 
    AVG(order_total) AS avg_order_value
FROM (
    SELECT o.order_id, SUM(oi.quantity * p.price) AS order_total
    FROM Orders o
    JOIN Order_Items oi ON o.order_id = oi.order_id
    JOIN Products p ON oi.product_id = p.product_id
    GROUP BY o.order_id
) sub;


-- 7. Repeat customers
-- Customers with more than 1 order.
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(o.order_id) AS orders_count
FROM Customers c
JOIN Orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, customer_name
HAVING COUNT(o.order_id) > 1
ORDER BY orders_count DESC;


-- 8. Average product rating
-- Uses Reviews table to show how products are rated.
SELECT 
    p.product_id,
    p.name,
    AVG(r.rating) AS avg_rating,
    COUNT(r.review_id) AS review_count
FROM Products p
JOIN Reviews r ON p.product_id = r.product_id
GROUP BY p.product_id, p.name
ORDER BY avg_rating DESC;


-- 9. Recent reviews (last 30 days)
-- Useful for customer

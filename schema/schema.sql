-- Customers
CREATE TABLE Customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products
CREATE TABLE Products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE Orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES Customers(customer_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'pending'
);

-- Order_Items (many-to-many between Orders and Products)
CREATE TABLE Order_Items (
    order_id INT NOT NULL REFERENCES Orders(order_id),
    product_id INT NOT NULL REFERENCES Products(product_id), 
    quantity INT NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (order_id, product_id)
);

-- Reviews
CREATE TABLE Reviews (
    review_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES Customers(customer_id),
    product_id INT NOT NULL REFERENCES Products(product_id),
    rating INT CHECK (rating BETWEEN 1 AND 5),
    comment TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Shipments
CREATE TABLE Shipments (
    shipment_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES Orders(order_id),
    shipped_date TIMESTAMP,
    delivery_date TIMESTAMP,
    shipping_method VARCHAR(50),
    status VARCHAR(20) DEFAULT 'in_transit'
);

CREATE TABLE IF NOT EXISTS customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    region TEXT,
    join_date TEXT,
    loyalty_points INTEGER,
    state TEXT
);

CREATE TABLE IF NOT EXISTS product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    stock_quantity INTEGER,
    year_added INTEGER,
    supplier TEXT
);

CREATE TABLE IF NOT EXISTS sale (
    transaction_id INTEGER PRIMARY KEY,
    sale_date DATE,
    customer_id INTEGER,
    product_id INTEGER,
    store_id INTEGER,
    sale_amount REAL,
    discount_percent REAL
    payment_type TEXT
   
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
    FOREIGN KEY (product_id) REFERENCES product (product_id)
);
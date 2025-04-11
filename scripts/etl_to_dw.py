import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

# Connect to SQLite – will create the file if it doesn't exist
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Create tables in the data warehouse if they don't exist."""
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS customer (
    customer_id INTEGER PRIMARY KEY,
    name TEXT,
    region TEXT,
    join_date TEXT,
    loyalty_points INTEGER,
    state TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS product (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    unit_price REAL,
    stock_quantity INTEGER,
    year_added INTEGER,
    supplier TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sale (
    transaction_id INTEGER PRIMARY KEY,
    sale_date DATE,
    customer_id INTEGER,
    product_id INTEGER,
    campaign_id INTEGER,
    store_id INTEGER,
    sale_amount REAL,
    discount_percent REAL,
    payment_type TEXT,
   
    FOREIGN KEY (customer_id) REFERENCES customer (customer_id),
    FOREIGN KEY (product_id) REFERENCES product (product_id)
    )
    """)
    

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sale tables."""
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM sale")


    

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
 # Rename columns in the DataFrame to match the database schema
    
    customers_df = customers_df.rename(columns={
        "CustomerID": "customer_id",
        "Name": "name",
        "Region": "region",
        "JoinDate": "join_date",
        "LoyaltyPoints": "loyalty_points",
        "State": "state"
    })
    """Insert customer data into the customer table."""
    customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
 # Rename columns in the DataFrame to match the database schema
    products_df = products_df.rename(columns={
        "productid": "product_id",
        "productname": "product_name",
        "category": "category",
        "unitprice": "unit_price",
        "stockquantity": "stock_quantity",
        "yearadded": "year_added",
        "Supplier": "supplier"
    })
    """Insert product data into the product table."""
    products_df.to_sql("product", cursor.connection, if_exists="append", index=False)

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
# Rename columns in the DataFrame to match the database schema
    sales_df = sales_df.rename(columns={
        "transactionid": "transaction_id",
        "saledate": "sale_date",
        "customerid": "customer_id",
        "productid": "product_id",
        "storeid": "store_id",
        "CampaignID": "campaign_id",
        "saleamount": "sale_amount",
        "discountpercent": "discount_percent",
        "paymenttype": "payment_type"

     })

    """Insert sales data into the sales table."""
    sales_df.to_sql("sale", cursor.connection, if_exists="append", index=False)

def load_data_to_db() -> None:
    try:
        

        # Create schema and clear existing records
        create_schema(cursor)
        delete_existing_records(cursor)

        # Load prepared data using pandas
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))


        # Insert data into the database
        insert_customers(customers_df, cursor)
        insert_products(products_df, cursor)
        insert_sales(sales_df, cursor)

        conn.commit()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    load_data_to_db()
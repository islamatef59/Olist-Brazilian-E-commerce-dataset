import ExtractData
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
load_dotenv()

# Access variables
DB_USER = os.getenv("USER")
DB_PASS = os.getenv("PASSWORD")
DB_NAME = os.getenv("DBNAME")
DB_HOST = os.getenv("HOST")
DB_PORT = os.getenv("PORT")
def load_tables_to_postgres(USER,PASSWORD,HOST,PORT,DBNAME,extract_data):

    DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"

# Create SQLAlchemy engine
    engine = create_engine(DATABASE_URL)


    tables = {
       "dim_customers": ExtractData.dim_customers,
       "dim_seller": ExtractData.dim_seller,
       "dim_products": ExtractData.dim_products,
       "dim_date": ExtractData.dim_date,
       "dim_payments": ExtractData.dim_payments,
       "fact_orders": ExtractData.fact_orders,
       "fact_orders_items": ExtractData.fact_order_items,
       "fact_payments": ExtractData.fact_payments,
       "fact_reviews": ExtractData.fact_reviews,
         }

    with engine.begin() as conn:
          for table_name in tables.keys():
           conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE "))

    for table_name,df in tables.items():
          print(f"Loading {table_name}...")
          df.to_sql(table_name,con= engine, if_exists="append", index=False)
    print(f" Table {table_name} loaded sucessfuly")

load_tables_to_postgres(
    USER=DB_USER,
    PASSWORD=DB_PASS,
    HOST=DB_HOST,
    PORT=DB_PORT,
    DBNAME=DB_NAME,
    extract_data=ExtractData
)
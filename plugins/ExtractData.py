import pandas as pd

# make DimDate Table from orders and reviews files
#passing a list of column names to parse_dates, pandas will automatically convert
# those columns into datetime objects during import.
orders=pd.read_csv("/opt/airflow/Resources/olist_orders_dataset.csv" ,parse_dates=[
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
])
reviews=pd.read_csv("/opt/airflow/Resources/olist_order_reviews_dataset.csv" ,parse_dates=[
    "review_creation_date",
    "review_answer_timestamp"
])

#print(orders)
#merge dates column into
all_dates=pd.concat([
orders['order_purchase_timestamp'],
orders['order_approved_at'],
orders['order_delivered_carrier_date'],
orders['order_delivered_customer_date'],
orders['order_estimated_delivery_date'],
reviews['review_creation_date'],
reviews['review_answer_timestamp']
])
unique_dates=pd.Series(all_dates.dropna().unique())
unique_dates=pd.to_datetime(unique_dates)
print(all_dates)
# build DimDate dimentional table

#merge all date columns into one dataframe
dim_date=pd.DataFrame({
    'date_id':unique_dates.dt.strftime("%Y%m%d").astype(int),
    'date':unique_dates,
    'year':unique_dates.dt.year,
    'quarter':unique_dates.dt.quarter,
    'month':unique_dates.dt.month,
    'month_name':unique_dates.dt.month_name(),
    'day':unique_dates.dt.day,
    'weekday':unique_dates.dt.weekday+1,
    'weekday_name':unique_dates.dt.day_name(),
    'is_weekend':unique_dates.dt.weekday >=5
}).sort_values("date").reset_index(drop=True)


#build DimCustomers dimention table from customers file
customers=pd.read_csv("/opt/airflow/Resources/olist_customers_dataset.csv")
dim_customers = customers.drop_duplicates(subset=["customer_id"]).reset_index(drop=True)
dim_customers['customer_sk']=dim_customers.index+1

dim_customers=dim_customers[[
    "customer_sk",
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]]
print(dim_customers)
dim_customers["customer_city"] = dim_customers["customer_city"].str.title()
dim_customers["customer_state"] = dim_customers["customer_state"].str.upper()
#print(dim_customers['customer_sk'])

# build products dimention tables
products=pd.read_csv("/opt/airflow/Resources/olist_products_dataset.csv")
translations=pd.read_csv("/opt/airflow/Resources/product_category_name_translation.csv")
products=products.merge(
          translations,
          how="left",
          on="product_category_name"
)
dim_products=products.drop_duplicates(subset=["product_id"]).reset_index(drop=True)

dim_products["product_category_name_english"] = dim_products[
    "product_category_name_english"
].fillna("unknown")

dim_products['product_sk']=dim_products.index+1
dim_products=dim_products[[
    "product_sk",
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]]

#print(dim_products['product_sk'])

#bulid seller dimention taable
sellers = pd.read_csv("/opt/airflow/Resources/olist_sellers_dataset.csv")
dim_seller=sellers.drop_duplicates(subset=['seller_id']).reset_index(drop=True)
dim_seller['seller_sk']=dim_seller.index+1
dim_seller=dim_seller[[
    "seller_sk",
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
]]

dim_seller['seller_city']=dim_seller['seller_city'].str.title()
dim_seller['seller_state']=dim_seller['seller_state'].str.upper()


#print(dim_seller['seller_sk'])

#bulid dim_payments table
payments = pd.read_csv("/opt/airflow/Resources/olist_order_payments_dataset.csv")
dim_payments=payments[["payment_type"]].drop_duplicates().reset_index(drop=True)
dim_payments["payment_sk"] = dim_payments.index + 1


#print(dim_payments)
#Building Review FactTable
reviews=pd.read_csv("/opt/airflow/Resources/olist_order_reviews_dataset.csv",parse_dates=[
"review_creation_date",
"review_answer_timestamp"
])
reviews['review_creation_date']=reviews['review_creation_date'].map(
      dim_date.set_index("date")["date_id"]
  )
reviews['review_answer_timestamp']=reviews['review_answer_timestamp'].map(
      dim_date.set_index("date")["date_id"]
)
fact_reviews=reviews[[
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    'review_creation_date',
    "review_answer_timestamp"

]]
#print(reviews['review_answer_timestamp'])
#print(fact_reviews.head())

#bulid oreder fact Table
orders=pd.read_csv("/opt/airflow/Resources/olist_orders_dataset.csv" ,parse_dates=[
"order_purchase_timestamp",
"order_approved_at",
"order_delivered_carrier_date",
"order_delivered_customer_date",
"order_estimated_delivery_date"
])
fact_orders = orders.copy()
fact_orders = fact_orders.merge(
    dim_customers[["customer_id", "customer_sk"]],
    on="customer_id",
    how="left"
)
fact_orders['purchase_date_id']=fact_orders['order_purchase_timestamp'].map(dim_date.set_index("date")['date_id'])
fact_orders['approved_date_id']=fact_orders['order_approved_at'].map(dim_date.set_index("date")['date_id'])
fact_orders['delivered_carrier_date_id']=fact_orders['order_delivered_carrier_date'].map(dim_date.set_index("date")['date_id'])
fact_orders['delivered_customer_date_id']=fact_orders['order_delivered_customer_date'].map(dim_date.set_index("date")['date_id'])
fact_orders['estimated_delivery_date_id']=fact_orders['order_estimated_delivery_date'].map(dim_date.set_index("date")['date_id'])
#print(fact_orders['delivered_carrier_date_id'])

fact_orders = fact_orders.drop(columns=["customer_id"], errors="ignore")
fact_orders["delivery_days"] = (orders["order_delivered_customer_date"] - orders["order_purchase_timestamp"]).dt.days
fact_orders["delay_days"]    = (orders["order_delivered_customer_date"] - orders["order_estimated_delivery_date"]).dt.days

fact_orders = fact_orders.drop(columns=["product_id","seller_id"], errors="ignore")
#print(fact_orders)
#bulid order_items fact Table
order_items=pd.read_csv("/opt/airflow/Resources/olist_order_items_dataset.csv",parse_dates=['shipping_limit_date'])

order_items['shipping_limit_date_id']=order_items['shipping_limit_date'].map(
    dim_date.set_index('date')['date_id'] )
fact_order_items=order_items.merge(
    dim_products[['product_sk','product_id']],
    on="product_id",
    how="left"
).merge(
    dim_seller[["seller_sk","seller_id"]],
    on="seller_id",
    how="left"
)

fact_order_items["product_sk"] = fact_order_items["product_sk"].fillna(0).astype(int)
fact_order_items["seller_sk"]  = fact_order_items["seller_sk"].fillna(0).astype(int)

# total cost of an item (price + freight/shipping)
fact_order_items["item_total"] = fact_order_items["price"] + fact_order_items["freight_value"]

fact_order_items=fact_order_items[[
    "order_id",
    "order_item_id",
    "product_sk",
    "seller_sk",
    "shipping_limit_date_id",
    "price",
    "freight_value"
]]
#print(fact_order_items)

#build  payments fact tables
payments = pd.read_csv("/opt/airflow/Resources/olist_order_payments_dataset.csv")
fact_payments=payments.merge(
    dim_payments,
    on="payment_type",
    how="left"
)
#indicate whether a payment was made in installments
fact_payments["is_installment"] = fact_payments["payment_installments"] > 1

fact_payments=fact_payments[[
    "order_id",
    "payment_sk",
    "payment_installments",
    "payment_value"
]]
def validate_star_schema(orders,order_items,payments,reviews,
                         fact_orders,fact_order_items,fact_reviews,fact_payments,
                         customers,products,sellers,
                         dim_customers,dim_products,dim_seller,dim_payments):
    print("=== Row Count Validation ===")
    print("Orders vs Fact Orders:", len(orders), "vs", len(fact_orders))
    print("Order Items vs Fact Order Items:", len(order_items), "vs", len(fact_order_items))
    print("Payments vs Fact Payments:", len(payments), "vs", len(fact_payments))
    print("Reviews vs Fact Fact Reviews:", len(reviews), "vs", len(fact_reviews))

    print("\n=== Surrogate Key Coverage (NaNs check) ===")
    if "customer_sk" in fact_orders:
          print("Missing customer_sk:", fact_orders["customer_sk"].isna().sum())
    if "purchase_date_id" in fact_orders:
         print("Missing purchase_date_id:", fact_orders["purchase_date_id"].isna().sum())
    if "payment_type_id" in fact_payments:
        print("Missing payment_type_id:", fact_payments["payment_type_id"].isna().sum())
    if "product_sk" in fact_order_items:
        print("Missing product_sk:", fact_order_items["product_sk"].isna().sum())

    print("\n=== Dimension Uniqueness Check ===")
    print("Unique customer_id in dim_customers:", dim_customers["customer_id"].nunique(), "/", len(dim_customers))
    print("Unique date in dim_date:", dim_date["date"].nunique(), "/", len(dim_date))
    print("Unique payment_type in dim_payments:", dim_payments["payment_type"].nunique(), "/", len(dim_payments))
    print("Unique product_id in dim_products:", dim_products["product_id"].nunique(), "/", len(dim_products))

    print("\n=== Foreign Key Coverage ===")
    missing_customers = set(fact_orders["customer_sk"]) - set(dim_customers["customer_sk"])
    print("Missing customers in dim_customers:", len(missing_customers))

    missing_dates = set(fact_orders["order_purchase_timestamp"].dt.date) - set(dim_date["date"])
    print("Missing dates in dim_date:", len(missing_dates))

    missing_payment_types = set(payments["payment_type"]) - set(dim_payments["payment_type"])
    print("Missing payment types in dim_payments:", len(missing_payment_types))

    missing_products = set(order_items["product_id"]) - set(dim_products["product_id"])
    print("Missing products in dim_products:", len(missing_products))

validate_star_schema(orders,order_items,payments,reviews,
                         fact_orders,fact_order_items,fact_reviews,fact_payments,
                         customers,products,sellers,
                         dim_customers,dim_products,dim_seller,dim_payments)

def cast_column_to_int(df, columns, dtype="int32"):
    if isinstance(columns,str):
        columns = [columns]
    for column in columns:
       if df[column].isna().sum()==0:
        df[column] = df[column].astype(dtype)
       else:
        print(f"⚠️ Column '{column}' has missing values, cannot cast to {dtype}.")
    return df

#cast fact tables
fact_orders = cast_column_to_int(fact_orders, ["customer_sk", "purchase_date_id"])
fact_order_items = cast_column_to_int(fact_order_items, ["product_sk","seller_sk"])

#cast dimention tables
dim_customers = cast_column_to_int(dim_customers, "customer_sk")
dim_products = cast_column_to_int(dim_products, "product_sk")
dim_seller = cast_column_to_int(dim_seller, "seller_sk")
dim_date = cast_column_to_int(dim_date, "date_id")


#print(fact_orders['customer_sk'].dtype)

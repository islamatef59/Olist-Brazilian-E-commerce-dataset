from operator import index

import pandas as pd
import ExtractData

#load dim Tables into files
ExtractData.dim_customers.to_csv(r"FilesLoaded\dim_customers.csv" , index=False)
ExtractData.dim_date.to_csv(r"FilesLoaded\dim_date.csv",index=False)
ExtractData.dim_seller.to_csv(r"FilesLoaded\dim_seller.csv",index=False)
ExtractData.dim_products.to_csv(r"FilesLoaded\dim_products.csv",index=False)
ExtractData.dim_payments.to_csv(r"FilesLoaded\dim_payments.csv",index=False)

#load Fact Tables into files
ExtractData.fact_orders.to_csv(r"FilesLoaded\fact_orders.csv" , index=False)
ExtractData.fact_order_items.to_csv(r"FilesLoaded\fact_order_items.csv" , index=False)
ExtractData.fact_payments.to_csv(r"FilesLoaded\fact_payments.csv" , index=False)
ExtractData.fact_reviews.to_csv(r"FilesLoaded\fact_reviews.csv" , index=False)
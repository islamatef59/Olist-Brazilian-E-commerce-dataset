

Brazilian E-Commerce Data Warehouse (ETL)
  Project Overview
This project implements a complete ETL (Extract, Transform, Load) pipeline to convert raw, transactional e-commerce data into a professional Star Schema optimized for Business Intelligence and Analytics. Using the Olist Brazilian E-Commerce Dataset, I transformed fragmented CSV files into a structured data warehouse model.

  Key Features
Star Schema Architecture: Designed and built 5 Dimension tables (Dim_Date, Dim_Customers, Dim_Products, Dim_Sellers, Dim_Payments) and 4 Fact tables.

Data Transformation: Implemented data cleaning, standardized geographic fields, and handled missing values using Python/Pandas.

Feature Engineering: Calculated business-critical metrics such as delivery_performance (actual vs. estimated delivery) and item_total_cost.

Data Integrity: Developed a validation suite to check for surrogate key coverage, referential integrity, and uniqueness across the warehouse.

Optimization: Performed memory optimization by casting data types (e.g., converting object IDs to int32 surrogate keys).


Technologies Used
Python 3.x

Pandas (Data Manipulation)

NumPy (Numerical Processing)

Airflow (Referenced in file paths for potential orchestration)

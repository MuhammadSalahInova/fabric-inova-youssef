# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "43ad46fb-abde-49fb-a8e2-f75e97532be8",
# META       "default_lakehouse_name": "Silver_Lakehouse",
# META       "default_lakehouse_workspace_id": "c31b15d8-8c12-4378-9b7b-7d9ef27fbfa2",
# META       "known_lakehouses": [
# META         {
# META           "id": "43ad46fb-abde-49fb-a8e2-f75e97532be8"
# META         },
# META         {
# META           "id": "0e4ecbb1-9b37-4842-b3bc-55559ece7baa"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F
spark.sql("""CREATE SCHEMA IF NOT EXISTS Gold_Lakehouse.Semantic""")

#Read tables
customers = spark.read.table("Silver_Lakehouse.Northwind.Customers")
orders = spark.read.table("Silver_Lakehouse.Northwind.Orders")
order_details = spark.read.table("Silver_Lakehouse.Northwind.Order_Details")
products = spark.read.table("Silver_Lakehouse.Northwind.Products")
categories = spark.read.table("Silver_Lakehouse.Northwind.Categories")
suppliers = spark.read.table("Silver_Lakehouse.Northwind.Suppliers")
employees = spark.read.table("Silver_Lakehouse.Northwind.Employees")
shippers = spark.read.table("Silver_Lakehouse.Northwind.Shippers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Filter most recent values for certian tables
order_details = order_details.filter(order_details["EndedAt"].isNull())
orders = orders.filter(orders["EndedAt"].isNull())
categories = categories.filter(categories["EndedAt"].isNull())
shippers = shippers.filter(shippers["EndedAt"].isNull())
suppliers = suppliers.filter(suppliers["EndedAt"].isNull())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Dimension Tables Preparation
#Customers + History
customers = customers.withColumn(
    "CustomerSK",
    F.xxhash64(F.col("RowHash"))
)
customers = customers.select(
    *[c for c in customers.columns if c not in ["RowHash", "IngestionSource"]]
)
customers = customers.withColumn(
    "IsCurrent",
    F.when(F.col("EndedAt").isNull(), True).otherwise(False)
)

#Employees + History
employees = employees.withColumn(
    "EmployeesSK",
    F.xxhash64(F.col("RowHash"))
)
employees = employees.select(
    *[c for c in employees.columns if c not in ["RowHash", "IngestionSource"]]
)
employees = employees.withColumn(
    "IsCurrent",
    F.when(F.col("EndedAt").isNull(), True).otherwise(False)
)

#Shippers
shippers = shippers.select(
    *[c for c in shippers.columns if c not in ["RowHash", "IngestionSource","IngestedAt","EndedAt"]]
)

#Categories
categories = categories.select(
    *[c for c in categories.columns if c not in ["RowHash", "IngestionSource","IngestedAt","EndedAt"]]
)

#Suppliers
suppliers = suppliers.select(
    *[c for c in suppliers.columns if c not in ["RowHash", "IngestionSource","IngestedAt","EndedAt"]]
)

#Products + History
products = products.withColumn(
    "ProductSK",
    F.xxhash64(F.col("RowHash"))
)
products = products.select(
    *[c for c in products.columns if c not in ["RowHash", "IngestionSource"]]
)
products = products.withColumn(
    "IsCurrent",
    F.when(F.col("EndedAt").isNull(), True).otherwise(False)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Date Dimension
# Get minimum and maximum OrderDate
dates = orders.select(
    F.min("OrderDate").alias("MinDate"),
    F.max("OrderDate").alias("MaxDate")
).collect()[0]

min_date = dates["MinDate"]
max_date = dates["MaxDate"]

# Create date range up to 2 years after maximum OrderDate
if not spark.catalog.tableExists("Gold_Lakehouse.Semantic.DimDate"):
    date_dim = spark.sql(f"""
        SELECT explode(
            sequence(
                to_date('{min_date}'),
                add_months(to_date('{max_date}'), 24),
                interval 1 day
            )
        ) AS Date
    """)
    date_dim = date_dim \
        .withColumn("DateSK", F.date_format("Date", "yyyyMMdd").cast("int")) \
        .withColumn("Year", F.year("Date")) \
        .withColumn("Quarter", F.quarter("Date")) \
        .withColumn("Month", F.month("Date")) \
        .withColumn("Day", F.dayofmonth("Date"))
    date_dim.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimDate")
else:
    date_dim = spark.read.table("Gold_Lakehouse.Semantic.DimDate")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Write Tables
customers.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimCustomers")
employees.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimEmployees")
shippers.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimShippers")
products.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimProducts")
categories.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimCategories")
suppliers.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.DimSuppliers")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Fact Table Denormalization
order_details = order_details.select(
    *[c for c in order_details.columns if c not in ["RowHash", "IngestionSource","IngestedAt","EndedAt"]]
)
orders = orders.select(
    *[c for c in orders.columns if c not in ["RowHash", "IngestionSource","IngestedAt","EndedAt"]]
)
#Join order with details
fact_sales = order_details.join(
    orders,
    on="OrderID",
    how="inner"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

#Historical Join----attempt only with actual dates
"""
fact_sales = fact_sales.alias("f").join(
    products.alias("p"),
    (F.col("f.ProductID") == F.col("p.ProductID")) &
    (F.col("f.OrderDate") >= F.col("p.IngestedAt")) &
    (
        (F.col("f.OrderDate") < F.col("p.EndedAt")) |
        F.col("p.EndedAt").isNull()
    ),
    "left"
)
"""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Fact Table keys
#Product
fact_sales = fact_sales.alias("f").join(
    products.filter(F.col("IsCurrent") == True).alias("p"),
    F.col("f.ProductID") == F.col("p.ProductID"),
    "left"
)
# Customer
fact_sales = fact_sales.join(
    customers.filter(F.col("IsCurrent") == True).alias("c"),
    F.col("f.CustomerID") == F.col("c.CustomerID"),
    "left"
)
# Employee
fact_sales = fact_sales.join(
    employees.filter(F.col("IsCurrent") == True).alias("e"),
    F.col("f.EmployeeID") == F.col("e.EmployeeID"),
    "left"
)
# Shippers
fact_sales = fact_sales.join(
    shippers.alias("s"),
    F.col("f.ShipVia") == F.col("s.ShipperID"),
    "left"
)
# Order Date
fact_sales = fact_sales.join(
    date_dim.alias("od"),
    F.to_date(F.col("f.OrderDate")) == F.col("od.Date"),
    "left"
)

# Shipped Date
fact_sales = fact_sales.join(
    date_dim.alias("sd"),
    F.to_date(F.col("f.ShippedDate")) == F.col("sd.Date"),
    "left"
)

# Required Date
fact_sales = fact_sales.join(
    date_dim.alias("rd"),
    F.to_date(F.col("f.RequiredDate")) == F.col("rd.Date"),
    "left"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Select Fact Sales
fact_sales = fact_sales.select(

    # Attributes
    F.col("f.OrderID").alias("OrderID"),
    F.col("f.ShipAddress").alias("ShipAddress"),
    F.col("f.ShipCountry").alias("ShipCountry"),
    F.col("f.ShipCity").alias("ShipCity"),
    F.col("f.ShipName").alias("ShipName"),

    # Keys
    F.col("f.ShipVia").alias("ShipperID"),
    F.col("p.ProductSK").alias("ProductSK"),
    F.col("c.CustomerSK").alias("CustomerSK"),
    F.col("e.EmployeesSK").alias("EmployeesSK"),
    
    #Dates
    F.col("od.DateSK").alias("OrderDateSK"),
    F.col("sd.DateSK").alias("ShippedDateSK"),
    F.col("rd.DateSK").alias("RequiredDateSK"),
    
    # Measures
    F.col("f.UnitPrice").alias("UnitPrice"),
    F.col("f.Quantity").alias("Quantity"),
    F.col("f.Discount").alias("Discount"),
    F.col("f.Freight").alias("Freight"),
    
    # Net Sales
    ((F.col("f.UnitPrice") * F.col("f.Quantity")) * (1 - F.col("f.Discount"))).alias("NetSales")
)

fact_sales.write.format("delta").mode("overwrite").saveAsTable("Gold_Lakehouse.Semantic.FactSales")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

Tables = "Orders"
State = "First"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import max,min,when,col,lit,to_date,current_date,when,date_add
from delta.tables import DeltaTable

#Check if nothing needs doing
if State == "Nothing":
    notebookutils.notebook.exit("Nothing")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Filter on most recent ingestion
df = spark.read.table(f"NorthWind.{Tables}")

if State == "First":
    filtered_df = df
else:
    max_value = df.select(max("IngestedAt")).first()[0]
    filtered_df = df.filter(col("IngestedAt") == max_value)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Generic Cleaning
#Drop null records
df = df.dropna(how="all")

#Handle null primary keys
if Tables == "Order_Details":
    pk = ["OrderID","ProductID"]
elif Tables == "Categories":
    pk = ["CategoryID"]
else:
    pk = [Tables[:-1] + "ID"]
filtered_df = filtered_df.dropna(subset=pk)

#Handle nullable Columns
#Rename Strings
filtered_df = filtered_df.fillna("Unknown")

#Replace numbers with mode
numeric_columns = [
    field.name
    for field in filtered_df.schema.fields
    if field.dataType.typeName() in ["integer", "long", "double", "float", "short", "decimal"]
]

for c in numeric_columns:
    mode_value = (
        filtered_df.filter(col(c).isNotNull())
          .groupBy(c)
          .count()
          .orderBy(col("count").desc())
          .first()[0]
    )

    if mode_value is not None:
        filtered_df = filtered_df.fillna({c: mode_value})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Specific Cleaning
#Employees
if Tables == "Employees":
    date_columns = ["HireDate", "BirthDate"]

    for column in date_columns:
        #Make it present
        filtered_df = filtered_df.withColumn(
            column,
            when(
                col(column) > current_date(),
                current_date()
            ).otherwise(col(column))
        )

        #Remove the gap
        filtered_df = filtered_df.withColumn(
        column,
        when(
            col("IngestionSource") == "API",
            date_add(col(column), 10336)
        ).otherwise(col(column))
        )

# Orders
if Tables == "Orders":

    # Correct RequiredDate if it is before OrderDate
    filtered_df = filtered_df.withColumn(
        "RequiredDate",
        when(
            col("RequiredDate") < col("OrderDate"),
            col("OrderDate")
        ).otherwise(col("RequiredDate"))
    )

    # Correct ShippedDate if it is before OrderDate
    filtered_df = filtered_df.withColumn(
        "ShippedDate",
        when(
            col("ShippedDate") < col("OrderDate"),
            col("OrderDate")
        ).otherwise(col("ShippedDate"))
    )

    # Remove the gap
    date_columns = ["OrderDate", "RequiredDate", "ShippedDate"]

    for column in date_columns:
        filtered_df = filtered_df.withColumn(
            column,
            when(
                col("IngestionSource") == "API",
                date_add(col(column), 10336)
            ).otherwise(col(column))
        )

    #Handle Countries
    filtered_df = filtered_df.withColumn(
    "ShipCountry",
    when(col("ShipCountry") == "USA", "United States")
    .when(col("ShipCountry") == "UK", "United Kingdom")
    .otherwise(col("ShipCountry"))
    )

#Order_Details
if Tables == "Order_Details":
    #Correct any wrong discounts
    filtered_df = filtered_df.withColumn(
    "Discount",
    when(col("Discount") < 0, 0)
    .when(col("Discount") > 1, 1)
    .otherwise(col("Discount"))
    )

    #Correct any negative values
    for c in ["Quantity","UnitPrice"]:
        min_value = filtered_df.select(min(col(c))).first()[0]

        filtered_df = filtered_df.withColumn(
            c,
            when(col(c) < 0, min_value).otherwise(col(c))
        )

#Overwrite
table = DeltaTable.forName(spark, f"NorthWind.{Tables}")

table.alias("target").merge(
    filtered_df.alias("source"),
    "target.RowHash = source.RowHash"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

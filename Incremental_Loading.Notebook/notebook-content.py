# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "57a47c02-b37e-402c-bc7f-2bab9f16b709",
# META       "default_lakehouse_name": "Bronze_Lakehouse",
# META       "default_lakehouse_workspace_id": "c31b15d8-8c12-4378-9b7b-7d9ef27fbfa2",
# META       "known_lakehouses": [
# META         {
# META           "id": "57a47c02-b37e-402c-bc7f-2bab9f16b709"
# META         },
# META         {
# META           "id": "43ad46fb-abde-49fb-a8e2-f75e97532be8"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

Tables = "Orders"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import max,col,lit,count,current_timestamp,to_date,when,add_months
import hashlib

spark.sql("""CREATE SCHEMA IF NOT EXISTS Silver_Lakehouse.NorthWind""")

#Check if table is ingested
if not spark.catalog.tableExists(f"Bronze_Lakehouse.Northwind.{Tables}"):
    notebookutils.notebook.exit("Nothing")

#Identify Table ID
if Tables == "Order_Details":
    IDchk = ""
elif Tables == "Categories":
    IDchk = "CategoryID"
else:
    IDchk = Tables[:-1] + "ID"

#constant current time
Time = current_timestamp()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read From Bronze
df = spark.read.table(f"Bronze_Lakehouse.Northwind.{Tables}")

#Retain one month only
df = df.filter(col("IngestedAt") >= add_months(current_timestamp(), -1))
df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"Bronze_Lakehouse.Northwind.{Tables}")

#Get latest ingested rows
Latest_Ingestion = df.select(max("IngestedAt")).collect()[0][0]
Updated_df = df.filter(df["IngestedAt"] == Latest_Ingestion)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Handle Relationships
#handle non existing categories
if Tables == "Products":
    Updated_df = Updated_df.withColumn(
    "CategoryID",
    when(col("CategoryID") > 8, 9)
    .otherwise(col("CategoryID"))
    )

if Tables == "Categories":
    ConcatenatedString = "9OthersAnything else"
    NewHashed = hashlib.sha256(ConcatenatedString.encode()).hexdigest()
    max_date = Updated_df.select(max("IngestedAt")).collect()[0][0]
    Updated_df = Updated_df.union(spark.createDataFrame([(9, "Others", "Anything else", NewHashed, "XLSX", max_date)], Updated_df.schema))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Simple Cleaning
#Turn string dates to dates
if Tables == "Employees":
    date_columns = ["HireDate", "BirthDate"]
    for column in date_columns:
        Updated_df = Updated_df.withColumn(
            column,
            to_date(col(column))
        )
if Tables == "Orders":
    date_columns = ["OrderDate", "RequiredDate","ShippedDate"]
    for column in date_columns:
        Updated_df = Updated_df.withColumn(
            column,
            to_date(col(column))
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Check if its the first time to apply incremental load
if not spark.catalog.tableExists(f"Silver_Lakehouse.Northwind.{Tables}"):
    #Perform SCD-2
    Updated_df = Updated_df.withColumn("EndedAt", lit(None))

    #Window By ID
    if Tables == "Order_Details":
        window = Window.partitionBy("OrderID", "ProductID")
    else:
        window = Window.partitionBy(IDchk)

    #Count ID Dublicates
    Updated_df = Updated_df.withColumn(
        "DuplicateCount",
        count("*").over(window)
    )

    #If its excel, ingested at = current
    Updated_df = Updated_df.withColumn(
        "IngestedAt",
        when(
            col("IngestionSource") == "XLSX",
            Time
        ).otherwise(col("IngestedAt"))
    )

    #If its not excel and has duplicates ended at = current
    Updated_df = Updated_df.withColumn(
        "EndedAt",
        when(
            (col("DuplicateCount") > 1) & (col("IngestionSource") != "XLSX"),
            Time
        ).otherwise(col("EndedAt"))
    )
    Updated_df = Updated_df.drop("DuplicateCount")

    #Save the table
    Updated_df.write.format("delta").mode("overwrite").saveAsTable(f"Silver_Lakehouse.Northwind.{Tables}")
    notebookutils.notebook.exit("First")


else:
    #Read Existing Data
    existing_df = spark.read.table(f"Silver_Lakehouse.Northwind.{Tables}")

    #Retain one year in silver
    existing_df = existing_df.filter(col("IngestedAt") >= add_months(current_timestamp(), -12))

    #Filter on new hashed values
    Updated_df = Updated_df.join(
    existing_df.select(
        col("RowHash").alias("old_RowHash")
    ),
    Updated_df["RowHash"] == col("old_RowHash"),
    "left_anti"
    )
    #If more than 20% terminate
    if Updated_df.count() > existing_df.count() * 0.20:
        existing_df.write.format("delta").mode("overwrite").saveAsTable(f"Silver_Lakehouse.Northwind.{Tables}")
        raise Exception(f"Bronze has more than 20% updated values")

    #If none append nothing
    if Updated_df.isEmpty():
        existing_df.write.format("delta").mode("overwrite").saveAsTable(f"Silver_Lakehouse.Northwind.{Tables}")
        notebookutils.notebook.exit("Nothing")
        
    #else Perform SCD-2
    else:
        Updated_df = Updated_df.withColumn("EndedAt", lit(None))
        #append new data to existing
        existing_df = existing_df.unionByName(Updated_df)


        #Window By ID
        if Tables == "Order_Details":
            window = Window.partitionBy("OrderID", "ProductID")
        else:
            window = Window.partitionBy(IDchk)

        #Count ID Dublicates
        existing_df = existing_df.withColumn(
            "DuplicateCount",
            count("*").over(window)
        )

        #Has duplicates and not new
        existing_df = existing_df.withColumn(
            "EndedAt",
            when(
                (col("DuplicateCount") > 1) & (col("EndedAt") == None) & (col("IngestedAt") != Updated_df.select("IngestedAt").first()[0]),
                Updated_df.select("IngestedAt").first()[0]
            ).otherwise(col("EndedAt"))
        )
        existing_df = existing_df.drop("DuplicateCount")

        #Save the table
        existing_df.write.format("delta").mode("overwrite").saveAsTable(f"Silver_Lakehouse.Northwind.{Tables}")
        notebookutils.notebook.exit("Normal")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

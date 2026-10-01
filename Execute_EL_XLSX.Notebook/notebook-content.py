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
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

SrcTable = "Products"
TrgtTable = "Products"
FilePath = "abfss://Northwind@onelake.dfs.fabric.microsoft.com/Bronze_Lakehouse.Lakehouse/Files/New/Excel/FakeStore_Orders_OrderDetails_Customers_Products_Linked.xlsx"
Time = "2026-09-03T00:40:15Z"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
from pyspark.sql.functions import to_timestamp,sha2, concat_ws,col,lit,lower,regexp_replace

# Initialization
spark.sql("""CREATE SCHEMA IF NOT EXISTS NorthWind""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Read xlsx with dataframe
xlsx = pd.read_excel(FilePath,sheet_name=SrcTable)
df = spark.createDataFrame(xlsx)

#Remove Unnecissary columns
df = df.drop("CreatedDate", "ModifiedDate", "Operation")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#New Hashed Column
#Create String
ConcatenatedString = concat_ws(
    "||",
    *[col(f"`{c}`") for c in df.columns]
)
#Normalize String
ConcatenatedString = lower(ConcatenatedString)
ConcatenatedString = regexp_replace(ConcatenatedString, " ", "")
ConcatenatedString = regexp_replace(ConcatenatedString, r"\.00", "")
#Hash it
NewHashed = sha2(ConcatenatedString,256)

df = df.withColumn("RowHash", NewHashed)

#New source identifier column
df = df.withColumn(
    "IngestionSource",
    lit("XLSX")
)

#New Ingestion time column
df = df.withColumn(
    "IngestedAt",
    to_timestamp(lit(Time))
)

#Extra handling for orders table dates = string
if SrcTable == "Orders":
    df = df.withColumns({
    "OrderDate": col("OrderDate").cast("string"),
    "RequiredDate": col("RequiredDate").cast("string"),
    "ShippedDate": col("ShippedDate").cast("string")
    })

#Write to bronze
df.write.format("delta").mode("append").saveAsTable(f"NorthWind.{TrgtTable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

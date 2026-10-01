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

SrcTable = ""
TrgtTable = ""
Base_URL = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import builtins
from pyspark.sql.functions import max,min,col,to_timestamp

#Specify Current Table ID
if SrcTable == "Order_Details":
    IDchk = "OrderID"
elif SrcTable == "Categories":
    IDchk = "CategoryID"
else:
    IDchk = SrcTable[:-1] + "ID"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Initialize Checker URLs
counturl = Base_URL+SrcTable+"/$count"
maxurl = Base_URL+SrcTable+f"?$orderby={IDchk}%20desc&$top=1"
minurl = Base_URL+SrcTable+f"?$orderby={IDchk}%20asc&$top=1"

#Return Count Response
response = requests.get(counturl)
countapi = int(response.text)

#Return Max Response
response = requests.get(maxurl)
data = response.json()
maxapi = data["value"][0][IDchk]

#Return Min Response
response = requests.get(minurl)
data = response.json()
minapi = data["value"][0][IDchk]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get latest ingested rows
df = spark.read.table(f"Northwind.{TrgtTable}")
Latest_Ingestion = df.select(max("IngestedAt")).collect()[0][0]

#Filter latest ingested rows from api
filtered_df = df.filter((df["IngestedAt"] == Latest_Ingestion) & (df["IngestionSource"] == "API"))

#Compare rows count
if filtered_df.count() == countapi:
    print("Records Match")
else:
    raise Exception(f"Ingested Records = {filtered_df.count()} While Actual = {count}")

#Compare min/max IDs
maxing = filtered_df.agg(max(col(IDchk))).collect()[0][0]
mining = filtered_df.agg(min(col(IDchk))).collect()[0][0]

if maxing == maxapi:
    print("Max ID Matches")
else:
    raise Exception(f"Ingested Max ID = {maxing} While Actual = {maxID}")

if mining == minapi:
    print("Min ID Matches")
else:
    raise Exception(f"Ingested Max ID = {mining} While Actual = {minID}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

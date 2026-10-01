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

Tables = ""
Message = ""
Stage = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import current_timestamp


df = spark.createDataFrame(
    [(Tables, Message, Stage)],
    ["TableName", "ErrorMessage", "Stage"]
)

df = df.withColumn("RunTime", current_timestamp())

df.write.format("delta").mode("append").saveAsTable("Configuration.Logs")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

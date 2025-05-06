# Databricks notebook source
# MAGIC %md
# MAGIC # Raw to datahub
# MAGIC This notebook accesses the silver config table, where it obtains a list of files in the raw layer and the name they will have in the silver layer, does some standard formatting, and saves them as delta tables.

# COMMAND ----------

# MAGIC %md
# MAGIC Imports:

# COMMAND ----------

from myFunctions import datahub_format

# COMMAND ----------

# MAGIC %md
# MAGIC ## get_dict_tables
# MAGIC Accesses the silver config table and retrieves the list of tables we are going to create in silver along with their corresponding locations in the raw layer

# COMMAND ----------

def get_dict_tables():
    df = spark.read.table("default.silver_config")
    print(f"Found {df.count()} tables in the silver config table")
    dic = {} 
    pandas_df = df.toPandas() 
    for column in pandas_df.columns: 
        dic[column] = pandas_df[column].values.tolist() 
    return(dic)

# COMMAND ----------

# MAGIC %md
# MAGIC ## get_from_raw
# MAGIC Given the path of a parquet file in the raw layer, returns it as a dataframe

# COMMAND ----------

def get_from_raw(raw_path):
    df = spark.read.format("parquet").load(raw_path)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## save_to_datahub
# MAGIC Given a dataframe and naming details, saves the dataframe to the silver layer

# COMMAND ----------

def save_to_datahub(df, schema, table):
    df.writeTo(f"default.{schema}_{table}").createOrReplace()

# COMMAND ----------

# MAGIC %md
# MAGIC ## table_raw_to_silver
# MAGIC Takes one table from raw to silver applying the necessary formatting

# COMMAND ----------

def table_raw_to_silver(location,schema, table):
    df = get_from_raw(location)
    df = datahub_format(df)
    save_to_datahub(df, schema, table)


# COMMAND ----------

# MAGIC %md
# MAGIC ## main
# MAGIC This function accesses the silver config table, where it obtains a list of files in the raw layer and the name they will have in the silver layer, does some standard formatting, and saves them as delta tables.

# COMMAND ----------

def main():
    relevant_tables_dic = get_dict_tables()
    num_tables = len(relevant_tables_dic["id"])
    for i in range(num_tables):
        path = relevant_tables_dic["raw_path"][i]
        schema = relevant_tables_dic["silver_schema"][i]
        table = relevant_tables_dic["silver_table_name"][i]
        print(f"Loading {schema}_{table} from {path}")
        table_raw_to_silver(path,schema, table)


# COMMAND ----------

main()
# Databricks notebook source
# MAGIC %md
# MAGIC # db_to_raw
# MAGIC This notebooks reads the bronze config table, where it obtains a list of databases and schemas, and loads the content of all databases and schemas to raw as parquet files

# COMMAND ----------

# MAGIC %md
# MAGIC ## get_dict_databases
# MAGIC Returns a dictionary with the name of all databases and schemas found in the bronze config table

# COMMAND ----------

def get_dict_databases():
    df = spark.read.table("default.bronze_config")
    df = df.filter(df.Source_type == 'db')
    print(f"Found {df.count()} databases in the bronze config table")
    
    dic = {} 
    pandas_df = df.toPandas() 
    for column in pandas_df.columns:
        dic[column] = pandas_df[column].values.tolist() 
    return(dic)


# COMMAND ----------

# MAGIC %md
# MAGIC ## get_list_of_tables
# MAGIC Given a database and a schema, returns a list of names of all tables in said schema

# COMMAND ----------

def get_list_of_tables(database, schema, user, pw):
    df = spark.read.format("jdbc")\
    .option("url", f"jdbc:sqlserver://nn-assignment-server.database.windows.net:1433;databaseName={database}")\
    .option("dbtable", "information_schema.tables")\
    .option("user", user)\
    .option("password", pw)\
    .load()\
    .filter(f"table_schema = '{schema}'")\

    l = [c[0] for c in df.select('TABLE_NAME').collect()]
    return l

# COMMAND ----------

# MAGIC %md
# MAGIC ## get_table
# MAGIC Given a database, schema and table name, returns the table as a dataframe

# COMMAND ----------

def get_table(database ,schema_name, table_name, user, pw):
  df = (spark.read
  .format("jdbc")
  .option("url", f"jdbc:sqlserver://nn-assignment-server.database.windows.net:1433;databaseName={database}")
  .option("dbtable", f"{schema_name}."+table_name)
  .option("user", user)
  .option("password", pw)
  .load()
  )
  print(f"Read {table_name} from {schema_name}")
  print(f"{df.count()} rows read")
  return df
  


# COMMAND ----------

# MAGIC %md
# MAGIC ## save_to_raw
# MAGIC Given a dataframe and a table name, saves the dataframe as a parquet file in the raw layer

# COMMAND ----------

def save_to_raw(df, table_name):
    df.write.format("parquet").mode('overwrite').save(f"dbfs:/raw/{table_name}")
    print(f"Saved {table_name} to raw")
    


# COMMAND ----------

# MAGIC %md
# MAGIC ## load_schema
# MAGIC Given a database and a schema, gets a list of tables, and iterates through the list to copy each table to the raw layer

# COMMAND ----------

def load_schema(database, schema, user, pw):
    list_of_tables = get_list_of_tables(database=database, schema=schema, user=user, pw=pw)
    print(list_of_tables)
    for table in list_of_tables:
        df = get_table( database=database,schema_name=schema ,table_name=table, user=user, pw=pw)
        save_to_raw(df=df, table_name=f"{schema}_{table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## main
# MAGIC This function reads the bronze config table, where it obtains a list of databases and schemas, and loads the content of all databases and schemas to raw as parquet files

# COMMAND ----------

def main():
    user = dbutils.secrets.get(scope = "jdbc", key = "dbUser")
    pw = dbutils.secrets.get(scope = "jdbc", key = "dbPass")
    dic = get_dict_databases()
    num_schemas = len(dic["Source_type"])
    for i in range(num_schemas):
        database = dic["Database_Name"][i]
        schema = dic["schema"][i]
        print(f"Loading {database} from {schema}")
        load_schema(database=database, schema=schema, user=user, pw=pw)


# COMMAND ----------

main()
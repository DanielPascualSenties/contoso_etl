def datahub_format(df):
    #Applies all transformations to ensure standard formatting in the silver layer. 
    #Right now it only sets column names to uppercase.
    for col in df.columns:
        df = df.withColumnRenamed(col, col.upper())
    return df
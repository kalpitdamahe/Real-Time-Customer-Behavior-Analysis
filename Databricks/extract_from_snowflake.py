from pyspark.sql import SparkSession
import os

def read_from_snowflake_table(database, schema, table):

    #Get sparkSession 
    spark = SparkSession.builder.getOrCreate()

    sfOptions = {
        "sfURL" : os.environ['SNOWFLKE_ACCOUNT'],
        "sfUser" : os.environ['SNOW_USER'],
        "sfPassword" : os.environ['SNOW_PASS'],
        "sfDatabase" : os.environ['SNOW_DB'],
        "sfSchema" : os.environ['SNOW_SCH'],
        "sfWarehouse" : os.environ['SNOW_WH']
        }
    
    df = spark.read.format('snowflake').options(**sfOptions).option('dbtable',f'{database}.{schema}.{table}').load()

    for c in df.columns:
        df = df.withColumnRenamed(c,c.lower())

    return df 

def read_from_snowflake_query(query):

    #Get sparkSession
    spark = SparkSession.builder.getOrCreate()

    sfOptions = {
        "sfURL" : os.environ['SNOWFLKE_ACCOUNT'],
        "sfUser" : os.environ['SNOW_USER'],
        "sfPassword" : os.environ['SNOW_PASS'],
        "sfDatabase" : os.environ['SNOW_DB'],
        "sfSchema" : os.environ['SNOW_SCH'],
        "sfWarehouse" : os.environ['SNOW_WH']
        }
    
    df = spark.read.format('snowflake').options(**sfOptions).option('query',query).load()

    for c in df.columns:
        df = df.withColumnRenamed(c,c.lower())

    return df
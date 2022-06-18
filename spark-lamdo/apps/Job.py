from pyspark.sql import SparkSession 
from pyspark import SparkConf
from pyspark.sql.types import DateType
from pyspark.sql.functions import col, count, when, isnan, max, sum ,to_date, year, month

#Connection details with PostgreSQL
PSQL_SERVERNAME = "192.168.0.122"
PSQL_PORTNUMBER = 5432
PSQL_DBNAME = "mydb"
PSQL_USRRNAME = "lamdo"
PSQL_PASSWORD = "lamdo1"
URL = f"jdbc:postgresql://{PSQL_SERVERNAME}:{PSQL_PORTNUMBER}/{PSQL_DBNAME}"

# Name_Table you want to create in the database 
TABLE_POSTGRES = "United_States"

# Connect SparkSession
conf = SparkConf()
path = "/opt/spark-data/postgresql-42.2.22.jar"
conf.set("spark.jars", path) 
spark = SparkSession.builder.config(conf=conf).appName("Covid").getOrCreate()

def extract_data(spark):
    path = "/opt/spark-data/data.csv"
    df = spark.read.option('inferSchema',True).option('header' , True).csv(path)
    return df

def transform_data(df):
    # Define Schema
    df_write = df.select('Date','Country','Commodity','Value','Cumulative')
    df_write = df_write.withColumn("Date",to_date(df_write["Date"],"dd/MM/yyyy"))
    df_write.printSchema()
    # Find all data of United States about Meat and edible offal.
    df_write = df_write.select(year(df_write["Date"]).alias("Year"),month(df_write["Date"]).alias("Month"),'*').\
                drop(df_write["Date"]).\
                filter((df_write.Country =="United States") & (df_write.Commodity=="Meat and edible offal"))
    '''
    #Check null:
    df_write.select([count(when(col(c).contains('Null') | \
                                (col(c) == '' ) | \
                                col(c).isNull() | \
                                isnan(c), c 
                               )).alias(c)
                        for c in df_write.columns]).show()
    '''

    # Caculator Value per month and year
    df_write = df_write.groupBy(df_write["Year"],df_write["Month"]).\
                        agg(sum("Value").alias("Value"),max("Cumulative").alias("Cumulative")).\
                        orderBy("Year", "Month")
    df_write.show()

    return(df_write)

def load_data(df_write):
    df_write.write\
        .format("jdbc")\
        .option("url", URL)\
        .option("dbtable", TABLE_POSTGRES)\
        .option("user", PSQL_USRRNAME)\
        .option("password", PSQL_PASSWORD)\
        .option("driver", "org.postgresql.Driver") \
        .save()
    print('-------------------')
    print('|Load Successfully|')
    print('-------------------')

if __name__ == "__main__":
    extract = extract_data(spark)
    transform = transform_data(extract)
    load_data(transform)

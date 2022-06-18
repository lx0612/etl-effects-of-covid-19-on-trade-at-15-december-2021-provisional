from pyspark.sql import SparkSession
from pyspark.sql.functions import col,date_format

def init_spark():
  sql = SparkSession.builder\
    .appName("economic")\
    .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\
    .getOrCreate()
  sc = sql.sparkContext
  return sql,sc

def main():
  url = "jdbc:postgresql://demo-database:5432/mta_data"
  properties = {
    "user": "postgres",
    "password": "casa1234",
    "driver": "org.postgresql.Driver"
  }
  file = "/opt/spark-data/custom_1988_2020.csv"
  sql,sc = init_spark()

  df = sql.read.load(file,format = "csv", inferSchema="true", sep="\t", header="true"
      )
  
  # Filter invalid coordinates
  df.write \
    .jdbc(url=url, table="economic", mode='append', properties=properties) \
    .save()
  
if __name__ == '__main__':
  main()

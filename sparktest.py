from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def session_open():
  
    spark = SparkSession \
               .builder \
               .appName("sparktest") \
               .master("local") \
               .config("spark.executor.memoryOverhead", "1g")  \
               .config("spark.driver.extraClassPath", "/home/nitishsingh/jars/mysql-connector-java-8.0.19.jar") \
               .config("spark.executor.extraClassPath", "/home/nitishsingh/jars/mysql-connector-java-8.0.19.jar") \
               .getOrCreate()
   
   
    return spark

spark = session_open()
print("SparkSession started")


df = spark.read.csv("mobile.csv", header='True', inferSchema='True')
df.show()
spark.stop()
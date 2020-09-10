import os 
os.environ["PYSPARK_SUBMIT_ARGS"] = ""

import findspark
findspark.init() 
# to install:
# $SPARK_HOME/bin/pyspark --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.0
# to load the mongoconnector package:
findspark.add_packages(["org.mongodb.spark:mongo-spark-connector_2.11:2.2.0"])

from pyspark.sql import SparkSession

FORMAT = "com.mongodb.spark.sql.DefaultSource"
URI = "mongodb://127.0.0.1:27017/{db}.{col}"

def get_session(database, collection):
    uri = URI.format(db=database, col=collection)
    return SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", uri) \
    .config("spark.mongodb.output.uri", uri) \
    .getOrCreate()
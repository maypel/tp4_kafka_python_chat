
from pyspark.sql import SparkSession
from pyspark.sql.functions import  *
from pyspark.sql.types import *

import time

kafka_topic_name = "chat_admin"
kafka_topic_bans = "chat_bans"
kafka_bootstrap_servers = "localhost:9092"

# Commande à lancer dans le terminal quand on est dans kafka_cours2
# /usr/local/opt/apache-spark/libexec/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 Projet_spark.py Projet_spark.py

if __name__ == "__main__" :

  ## Création d'uns session spark.sql
  spark = SparkSession \
    .builder \
    .appName("Projet_Spark_Kafka") \
    .master("local[*]") \
    .getOrCreate()

  # Filtration des infos reçu par le flux
  spark.sparkContext.setLogLevel("Warn")

  # Lecture du stram provenant de kafka
  df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic_name) \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .load()

  # Me sélectionne dans les données, la clé, la valeur et le timestamp
  df=df.selectExpr("CAST(key as STRING)", "CAST(value as STRING)", "CAST(timestamp as TIMESTAMP)")
  # Me scinde la value en name + message
  df = df.withColumn('name', translate(split(split(df["value"], ',').getItem(0), ':').getItem(1), '"', ''))
  df = df.withColumn('message', translate(split(split(df["value"], ',').getItem(1), ':').getItem(1), '"', ''))

  # Filter par le nom, message => spam
  query = df.groupBy(window("timestamp", "60 seconds"), "name", "message").count()
  query = query.filter(query["count"] > 10)
  query = query.withColumn('message', lit('spam de message'))
  query = query.withColumn('send_msg', concat(col('name'), lit(":"), col('message')))

  query.select(col("send_msg").alias("value")) \
    .selectExpr("CAST(value as STRING)") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("topic", kafka_topic_bans) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start() \
    .awaitTermination()

## Spark Streaming — Kafka to Spark Pipeline (Introduction Lab)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum , to_timestamp , window
from pyspark.sql.types import StructType, StructField, StringType, FloatType , TimestampType
from pyspark.sql.functions import length
from pyspark.sql.functions import col, sum as spark_sum
import matplotlib.pyplot as plt


def main():


    # TODO 2: Create a SparkSession
    spark = SparkSession.builder \
        .appName("KafkaSparkStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1") \
        .getOrCreate()


    schema = StructType([
        StructField("transaction_id", StringType()),
        StructField("user_id", StringType()),
        StructField("amount", FloatType()),
        StructField("timestamp", StringType())
    ])
    


    # TODO 3: Define Kafka connection parameters
    kafka_bootstrap_servers = "localhost:9092"
    kafka_topic = "topic_csv"
    


    # TODO 4: Read data from Kafka as a streaming DataFrame
    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .load()
   


    # TODO 5: Inspect the schema of the streaming DataFrame
    #kafka_df.printSchema()



    # TODO 6: Convert the Kafka message value from bytes to string
    # TODO 7: Apply a simple transformation
    

    json_df = kafka_df.selectExpr("CAST(value AS STRING) as value") \
                  .select(from_json(col("value"), schema).alias("data")) \
                  .select("data.*") \
                  .withColumn("amount", col("amount").cast("float")) \
                  .withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))


    agg_df = json_df.withWatermark("timestamp", "5 seconds") \
                .groupBy(window("timestamp", "5 seconds"), "user_id") \
                .agg(spark_sum("amount").alias("total_amount"))
    #agg_df = json_df.groupBy("user_id").agg(spark_sum("amount").alias("total_amount"))
    #agg_df = json_df.withColumn("transaction_id_length", length(col("transaction_id")))



    # TODO 8: Write the streaming output
    query = agg_df.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .queryName("agg_table") \
        .start()

    df = spark.sql("SELECT * FROM agg_table").toPandas()

    plt.bar(df['user_id'], df['total_amount'])
    plt.show()

    # TODO 9: Keep the streaming query running
    query.awaitTermination()
   


    # TODO 10: Gracefully stop the Spark session
    #spark.stop()



if __name__ == "__main__":
    main()

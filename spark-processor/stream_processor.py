from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

schema = StructType([
    StructField("location", StringType()),
    StructField("country", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("parameter_id", IntegerType()),
    StructField("parameter", StringType()),
    StructField("unit", StringType()),
    StructField("sensor_id", IntegerType()),
    StructField("timestamp", StringType())
])

# Create Spark session
spark = SparkSession.builder \
    .appName("ResearthStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "researth-kafka:9092") \
    .option("subscribe", "environmental-data") \
    .load()

messages = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Print to console
query = messages.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
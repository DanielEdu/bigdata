# https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from schemas import sch_json


spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('spark_streaming_app')
    ).getOrCreate()

spark.sparkContext.setLogLevel('WARN')


df_s = spark.readStream.format("kafka").\
    option("kafka.bootstrap.servers", "localhost:9092").\
    option("subscribe", "quickstart-events").\
    option("startingOffsets", "latest").\
    load()


df_cast = df_s.select(df_s['value'].cast('string'), df_s['offset'], df_s['timestamp'])


# df_flat = (df_cast
#       .withColumn('json_data', f.from_json(df_cast['value'], schema=sch_json))
#       .select('json_data','offset','timestamp','json_data.message', 'json_data.timestamp')
#     )


query = (df_s
    .writeStream
    .outputMode("update")
    .trigger(processingTime="5 seconds")
    .format("console")
    .option('truncate', False)
    .start()
)

query.awaitTermination()
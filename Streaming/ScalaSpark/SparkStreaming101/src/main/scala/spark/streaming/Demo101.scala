package spark.streaming

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{from_json, col}
import Utils.{sparkInfo, sch_json}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}


object Demo101 extends  App {

  val sch_json_2: StructType = StructType(Array(
    StructField("iss_position", StructType(Array(
      StructField("latitude", StringType, nullable = true),
      StructField("longitude", StringType, nullable = true)
    )), nullable = true),
    StructField("message", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("SparkStreamingDemo")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  sparkInfo(spark)


  val dfStream = spark.readStream.format("kafka").
    option("kafka.bootstrap.servers", "localhost:9092").
    option("subscribe", "quickstart-events").
    option("startingOffsets", "latest").
    load()


  val dfStream_cast = dfStream.select(
    col("value").cast("string"),
    col("offset"),
    col("timestamp")
  )


  val df_final = dfStream_cast
    .withColumn("json_Data", from_json(col("value"),sch_json_2))
    .select(
      col("offset"),
      col("timestamp"),
      col("json_data.iss_position.latitude"),
      col("json_data.iss_position.longitude"),
      col("json_data.message"),
      col("json_data.timestamp")
    )
    .drop("value")



  val query = (df_final
    .writeStream
    .outputMode("update")
    .trigger(Trigger.ProcessingTime("5 seconds"))
    .option("truncate", value = false)
    .format("console")
    .start()
  )

  query.awaitTermination()

}

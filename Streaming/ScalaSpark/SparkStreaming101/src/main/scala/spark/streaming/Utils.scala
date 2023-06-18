package spark.streaming
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Utils {

  val sch_json: StructType = StructType(Array(
    StructField("iss_position", StructType(Array(
      StructField("latitude", StringType, nullable = true),
      StructField("longitude", StringType, nullable = true)
    )), nullable = true),
    StructField("message", StringType, nullable = true),
    StructField("timestamp", LongType, nullable = true)
  ))

  def sparkInfo(spark: SparkSession): Unit = {
    println("Hello, SparkDemo")

    val sparkVersion = spark.version
    val scalaVersion = util.Properties.versionNumberString
    val javaVersion = System.getProperty("java.version")

    println("SPARK VERSION = " + sparkVersion)
    println("SCALA VERSION = " + scalaVersion)
    println("JAVA  VERSION = " + javaVersion)
  }

}

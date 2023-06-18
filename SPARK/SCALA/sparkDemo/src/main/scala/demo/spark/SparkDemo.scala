package demo.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkDemo extends App {

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("SparkDemo")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val sparkVersion = spark.version
  val scalaVersion = util.Properties.versionNumberString
  val javaVersion = System.getProperty("java.version")

  println("SPARK VERSION = " + sparkVersion)
  println("SCALA VERSION = " + scalaVersion)
  println("JAVA  VERSION = " + javaVersion)

  val df_pokemon = read_df("/Users/Shared/data/pokemon.csv")
  df_pokemon.show(5, truncate = false)
  write_df(df_pokemon, "pokemon_data")


  val df_movies = read_df("/Users/Shared/data/movie.csv")
  df_movies.show(5, truncate = false)
  write_df(df_movies, "movies_data")


  def read_df(path: String): DataFrame = {
    val df = spark.read.format ("csv")
      .option ("header", "true")
      .option ("inferSchema", "true")
      .load (path)
    df
  }

  def write_df(df: DataFrame, path: String): Unit = {
    df.write.format("csv")
      .mode("overwrite")
      .option("header", "true")
      .save("./output/" + path)
  }




  spark.close()
}

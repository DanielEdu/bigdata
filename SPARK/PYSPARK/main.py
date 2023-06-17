from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('my_spark_app')
    ).getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# Leemos el archivo de datos en una variable
df_pokemon = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Users/Shared/data/pokemon.csv")
    )


df_json = (
    spark.read.format("json")
    .option("inferSchema", "true")
    .load("/Users/Shared/data/data_test.json")
    )


df1 = df_pokemon.filter(df_pokemon["Legendary"] == True)
df1.show(5)


df1.write.format("csv") \
        .mode("overwrite") \
        .option("header", "true") \
        .save("./salida/")


spark.stop()
exit()
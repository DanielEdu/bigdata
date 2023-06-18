# Pyspar Sparke Structured Streamig

Para ejecutar un proceso Pyspark que consuma un Topic de Kafka deberá cargar el paquete: `org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2` (*dependiendo de su versión de Spark*)

```bash
$spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 main.py
```

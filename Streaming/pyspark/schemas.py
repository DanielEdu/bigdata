from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType, DoubleType, LongType


sch_json = StructType([
    StructField('iss_position', StructType([
        StructField('latitude', StringType(), True), 
        StructField('longitude', StringType(), True)
        ]), True), 
    StructField('message', StringType(), True), 
    StructField('timestamp', LongType(), True)]
    )

from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType

def customer_struct():
    return StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("active", DateType(), True)
    ])
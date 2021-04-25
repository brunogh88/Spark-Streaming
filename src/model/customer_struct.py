from pyspark.sql.types import StructField, StructType, StringType, IntegerType

def customer_struct():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("customerName", StringType(), True)
    ])
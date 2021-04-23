from pyspark.sql.types import StructField, StructType, StringType, IntegerType

def payment_type_struct():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("paymentType", StringType(), True)
    ])
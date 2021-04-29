from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DateType, DoubleType

def sales_struct():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("salesDate", DateType(), True),
        StructField("idPaymentType", IntegerType(), True),
        StructField("idCustomer", IntegerType(), True),
        StructField("productName", StringType(), True),
        StructField("amount", DoubleType(), True)
    ])
    
from pyspark.sql import functions as F

class SalesQuality():

    def __init__(self, spark_sesseion):
        self.spark_session = spark_sesseion


    def qualityData(self, df):
        return (
            df
                .withColumn(
                    "id",
                    F.when(F.col("id").isNull(), F.lit(-99999))
                    .otherwise(F.col("id")))
        )
        
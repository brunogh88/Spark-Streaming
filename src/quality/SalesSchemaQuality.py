from pyspark.sql import functions as F
from src.model.sales_struct import sales_struct

class SalesSchemaQuality(object):

    def __init__(self):
        pass

    def process(self, df):
        """
            Args: DataFrame Sales
        """
        return(
            df
            .select(F.from_json(F.col("value"), sales_struct()).alias("all"))
            .select("all.*")
        )
        
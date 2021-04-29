import pytest
import pyspark

from src.quality.SalesSchemaQuality import SalesSchemaQuality

def teste(spark):
    json = """{\"id\": 1,
               \"salesDate\": \"2021-04-25\", 
               \"idPaymentType\": 1, 
               \"idCustomer\": 1, 
               \"productName\": \"Mouse\", 
               \"amount\": 15.00}"""

    dataframe = spark.parallelize(json)

    df_sales = SalesSchemaQuality().process(dataframe)

    assert df_sales.coun() > 0
    
from importlib import import_module
from pyspark.sql import functions as F

sales_load = import_module("src.load.SalesLoad").SalesLoad
#from src.load import SalesLoad
sales_save = import_module("src.save.SalesSave").SalesSave
sales_quality = import_module("src.quality.SalesQuality").SalesQuality
payment_type_load = import_module("src.load.PaymentTypeLoad").PaymentTypeLoad
join_sales_payment_type = import_module("src.process.JoinSalesAndPaymentType").JoinSalesAndPaymentType

class SalesPipe(object):
    """Classe que contém todos os passos para a pipe SALES"""

    def __init__(self, spark):
        self.sales_load = sales_load(spark)
        self.sales_save = sales_save(spark)
        self.sales_quality = sales_quality(spark)
        self.payment_type_load = payment_type_load(spark)
        self.join_sales_payment_type = join_sales_payment_type(spark)

    def start(self):
        df_sales = self.sales_load.read_stream_sales()
        df_sales = df_sales.withColumn("dt_partition", F.lit(20210420))
        df_payment_type = self.payment_type_load.load_payment_type()

        df_sales = self.sales_quality.qualityData(df_sales)

        self.sales_save.save(df_sales)

        teste = self.join_sales_payment_type.process(df_sales, df_payment_type)

        teste.show()
        
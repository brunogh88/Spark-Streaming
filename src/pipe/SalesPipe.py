from pyspark.sql import functions as F

from src.load.SalesLoad import SalesLoad
from src.save.SalesSave import SalesSave
from src.quality.SalesQuality import SalesQuality
from src.load.PaymentTypeLoad import PaymentTypeLoad
from src.process.JoinSalesAndPaymentType import JoinSalesAndPaymentType

class SalesPipe(object):
    """Classe que contém todos os passos para a pipe SALES"""

    def __init__(self, spark):
        self.sales_load = SalesLoad(spark)
        self.sales_save = SalesSave(spark)
        self.sales_quality = SalesQuality(spark)
        self.payment_type_load = PaymentTypeLoad(spark)
        self.join_sales_payment_type = JoinSalesAndPaymentType(spark)

    def start(self):
        df_sales = self.sales_load.read_stream_sales()
        df_sales = df_sales.withColumn("dt_partition", F.lit(20210420))
        df_payment_type = self.payment_type_load.load_payment_type()

        df_sales = self.sales_quality.qualityData(df_sales)

        self.sales_save.save(df_sales)

        teste = self.join_sales_payment_type.process(df_sales, df_payment_type)

        teste.show()

        
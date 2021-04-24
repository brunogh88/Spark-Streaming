from pyspark.sql.functions import col, lit
from src.load.SalesLoad import SalesLoad
from src.save.SalesSave import SalesSave
from src.quality.SalesQuality import SalesQuality
from src.load.PaymentTypeLoad import PaymentTypeLoad
from src.process.JoinSalesAndPaymentType import JoinSalesAndPaymentType
from src.ingest.SalesIngest import SalesIngest
from src.utils import log

class SalesPipe(object):
    """Classe que contém todos os passos para a pipe SALES"""

    def __init__(self, spark, args):
        self.spark_session = spark
        self.args = args
        self.df_sales = None
        self.df_payment_type = None

    def loadStep(self):
        self.df_sales = SalesLoad(self.spark_session).read_stream_sales()
        self.df_sales = self.df_sales.withColumn("dt_partition", lit(self.args[1]))
        self.df_payment_type = PaymentTypeLoad(self.spark_session).load_payment_type()

    def qualityStep(self):
        self.df_sales = SalesQuality(self.spark_session).qualityData(self.df_sales)

    def saveStep(self):
        SalesSave(self.spark_session).save(self.df_sales)

    def processStep(self):
        self.df_sales = JoinSalesAndPaymentType(self.spark_session).process(self.df_sales, self.df_payment_type)

    def ingestStep(self):
        SalesIngest().save(self.df_sales)

    def start(self):
        self.loadStep()
        self.qualityStep()
        self.saveStep()
        self.processStep()
        self.ingestStep()

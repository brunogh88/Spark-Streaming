from pyspark.sql import functions as F
from src.load.SalesLoad import SalesLoad
from src.load.PaymentTypeLoad import PaymentTypeLoad
from src.save.SalesSave import SalesSave
from src.quality.SalesQuality import SalesQuality
from src.quality.SalesSchemaQuality import SalesSchemaQuality
from src.process.JoinSalesAndPaymentTypeProcess \
    import JoinSalesAndPaymentTypeProcess
from src.process.JoinSalesAndCustomerProcess import JoinSalesAndCustomerProcess
from src.process.RenameColumnsForOracleProcess \
    import RenameColumnsForOracleProcess
from src.ingest.SalesIngest import SalesIngest
from src.ingest.SalesOracleIngest import SalesOracleIngest
from src.load.CustomerLoad import CustomerLoad

class SalesPipe(object):
    """Classe que contém todos os passos para a pipe SALES"""

    def __init__(self, spark, args):
        self.spark_session = spark
        self.args = args
        self.df_sales = None
        self.df_payment_type = None
        self.df_customer = None
        self.df_sales_oracle = None

    def __loadStep(self):
        self.df_sales = SalesLoad(self.spark_session).read_stream_sales()
        self.df_payment_type = \
            PaymentTypeLoad(self.spark_session).load_payment_type()
        self.df_customer = CustomerLoad(self.spark_session).load()

    def __qualityStep(self):
        self.df_sales = SalesSchemaQuality().process(self.df_sales)
        self.df_sales = \
            SalesQuality(self.spark_session).qualityData(self.df_sales)
        self.df_sales = \
            self.df_sales.withColumn("dt_partition", F.lit(self.args.date))

    def __saveStep(self):
        SalesSave(self.spark_session).save(self.df_sales)

    def __processStep(self):
        self.df_sales = \
            JoinSalesAndPaymentTypeProcess() \
                .process(self.df_sales, self.df_payment_type)
        self.df_sales = \
            JoinSalesAndCustomerProcess() \
                .process(self.df_sales, self.df_customer)
        self.df_sales_oracle = \
            RenameColumnsForOracleProcess().process(self.df_sales)

    def __ingestStep(self):
        SalesIngest().save(self.df_sales)
        SalesOracleIngest().save(self.df_sales_oracle)

    def start(self):
        self.__loadStep()
        self.__qualityStep()
        self.__saveStep()
        self.__processStep()
        self.__ingestStep()

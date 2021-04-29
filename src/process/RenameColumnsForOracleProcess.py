from pyspark.sql import functions as F
from pyspark.sql.types import DateType, StringType

class RenameColumnsForOracleProcess(object):
    """
        Classe responsável por renomear as colunas iguais aos da tabela TB_SALES do Oracle
    """
    def __init__(self):
        pass

    def process(self, df):
        return (
                    df
                    .withColumnRenamed( "id", "ID")
                    .withColumnRenamed( "salesDate", "DT_SALES")
                    .withColumnRenamed( "idPaymentType", "ID_PAYMENT_TYPE")
                    .withColumnRenamed( "idCustomer", "ID_CUSTOMER")
                    .withColumnRenamed( "productName", "NM_PRODUCT")
                    .withColumnRenamed( "amount", "VL_AMOUNT")
                    .withColumnRenamed( "paymentType", "NM_PAYMENT_TYPE")
                    .withColumnRenamed( "customerName", "NM_CUSTOMER")
                    .withColumnRenamed( "dt_partition", "DT_PROCESSING")
                    .withColumn("DT_PROCESSING", F.to_date(F.col("DT_PROCESSING").cast(StringType()), "yyyyMMdd"))
                )
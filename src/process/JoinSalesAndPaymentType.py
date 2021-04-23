

class JoinSalesAndPaymentType():

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def process(self, df_sales, df_payment_type):
        return df_sales.join(df_payment_type, df_sales.idPaymentType == df_payment_type.id, "INNER")

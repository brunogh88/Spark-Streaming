

class JoinSalesAndPaymentTypeProcess(object):

    def __init__(self):
        pass

    def process(self, df_sales, df_payment_type):
        return df_sales.join(
            df_payment_type, 
            df_sales.idPaymentType == df_payment_type.id, 
            "INNER").drop(df_payment_type.id)

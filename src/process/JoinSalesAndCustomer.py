
class JoinSalesAndCustomer(object):
    
    def __init__(self):
        pass

    def process(self, df_sales, df_customer):
        return df_sales.join(df_customer, df_sales.idCustomer == df_customer.id).drop(df_customer.id)
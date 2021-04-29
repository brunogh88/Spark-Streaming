from src.utils import hdfs_utils
from src.utils.config import config
from src.model.payment_type_struct import payment_type_struct
from src.utils import log

class PaymentTypeLoad(object):
    """Payment Type Load

    Args:
        spark_session: Spark Session
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session
        self.hdfs_utils = hdfs_utils.HdfsUtils(spark_session)

    def load_payment_type(self):
        log.info("Carregando os dados de tipo de pagamento")
        return self.hdfs_utils.read(
            config("SPARK_RAW_PAYMENT_TYPE_PATH"), 
            payment_type_struct(), config("SPARK_RAW_FORMAT")
            )
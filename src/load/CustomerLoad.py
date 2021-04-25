from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config
from src.model.customer_struct import customer_struct

class CustomerLoad(object):
    
    def __init__(self, spark_session):
        self.spark_session = spark_session

    def load(self):
        return HdfsUtils(self.spark_session).read(
                    path=config("SPARK_TRUSTED_PATH")+config("CUSTOMER_PATH"),
                    schema=customer_struct(),
                    format=config("SPARK_PARQUET_FORMAT")
        )

    
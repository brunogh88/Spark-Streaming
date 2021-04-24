from src.utils.config import config
from src.utils.hdfs_utils import HdfsUtils

class SalesSave(object):
    """
    Classe responsável por fazer a persistencia do
    stream no HDFS
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def save(self, df):
        """
        Persiste o dataframe de vendas

        :param df:
        """
        HdfsUtils(None).writeStream(
            df=df, 
            path=config("SPARK_TRUSTED_PATH") + config("SALES_PATH"),
            format=config("SPARK_TRUSTED_FORMAT"),
            partition_name="dt_partition",
            save_mode=config("SPARK_TRUSTED_MODE"),
            check_point_path=config("SPARK_TRUSTED_PATH") + config("SALES_PATH") + config("CHECKPOINT_PATH"),
            query_name="sales-save-trusted"
            )

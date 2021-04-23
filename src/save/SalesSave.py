# -*- coding: utf-8 -*-

from importlib import import_module

config = import_module("src.utils.config").config


class SalesSave():
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
        dfs = (
            df.writeStream.queryName("sales-test")
                .outputMode(config("SPARK_TRUSTED_MODE"))
                .partitionBy("dt_partition")
                .format(config("SPARK_TRUSTED_FORMAT"))
                .option("path", config("SPARK_TRUSTED_PATH") + config("SALES_PATH"))
                .option("checkpointLocation", config("SPARK_TRUSTED_PATH") + config("SALES_PATH") + config("CHECKPOINT_PATH"))
                .option("truncate", False)
                .start()
        )
        dfs.awaitTermination()

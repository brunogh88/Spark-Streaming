"""
spark_utils.py
~~~~~~~~

Módulo que contém a função auxiliar para uso com o Apache Spark
"""
from pyspark.sql import SparkSession
from src.utils.config import config

def get_spark_session(pipe):
    """
    Inicia uma sessão do spark

    :return: SparkSession
    """
    return SparkSession.builder.appName(
        config("APP_NAME")+"_"+pipe
    ).getOrCreate()

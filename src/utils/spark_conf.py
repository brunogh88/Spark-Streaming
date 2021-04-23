# -*- coding: utf-8 -*-
"""
spark_utils.py
~~~~~~~~

Módulo que contém a função auxiliar para uso com o Apache Spark
"""
from pyspark.sql import SparkSession
from importlib import import_module

config = import_module("src.utils.config").config

def get_spark_session(module):
    """
    Inicia uma sessão do spark

    :return: SparkSession
    """
    return SparkSession.builder.appName(
        config("APP_NAME")+"_"+module
    ).getOrCreate()

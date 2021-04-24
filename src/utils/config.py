# -*- coding: utf-8 -*-

import os

def config(key):
    """
    Classe responsavel por carregar configuração para o projeto
    OBS.: não foi implemntado o recebimento do
    ENVIRONMENT via spark

    :param key: chave da configurações
    :return: valor da configurações
    """
    config = {
                "APP_NAME" : 'sales-test',

                "KAFKA_BOOTSTRAP_SERVERS" : 'localhost:29092',
                "KAFKA_TOPIC" : 'teste',
                "KAFKA_OFFSET_RESET_START_OFFSET": "earliest",
                "KAFKA_TOPIC_LOG": "log-monitor",
                "KAFKA_AUTHORIZATION_CLIENT_ID": "sales-test",
                "KAFKA_GROUP_ID": "sales-test",
                "KAFKA_SESSION_TIMEOUT_MS": 30000,
                "KAFKA_HEARTBEAT_INTERVAL_MS": 3000,
                "KAFKA_REQUEST_TIMEOUT": 40000,
                "KAFKA_CONNECTION_MAX_IDLE_MS": 540000,
                "KAFKA_POOL_TIMEOUT_MS": 512,
                "KAFKA_FAIL_ON_DATA_LOSS": True,
                "KAFKA_NUMBER_RETRIES": 3,
                "KAFKA_RETRIES_INTERVAL_MS": 1000,
                "KAFKA_MAX_OFFSETS_PER_TRIGGER": 1000,
                "KAFKA_MIN_BATCHES_RETAIN": 100,

                "SPARK_RAW_FORMAT": "csv",
                "SPARK_RAW_PAYMENT_TYPE_PATH": "./src/data/raw/payment_type",

                "SPARK_TRUSTED_MODE" : "append",
                "SPARK_TRUSTED_FORMAT" : "parquet",
                "SPARK_TRUSTED_PATH" : "./src/data/trusted",

                "CHECKPOINT_PATH" : "/checkpoint",
                "SALES_PATH" : "/sales",
                "CUSTOMER_PATH" : "/customer",
                "PAYMENT_TYPE_PATH" : "/payment_type",

                "SPARK_REFINED_PATH" : "./src/data/refined",

                "SPARK_OVERWRITE_MODE" : "Overwrite",
                "SPARK_PARQUET_FORMAT" : "parquet"
    }
    return config[key]
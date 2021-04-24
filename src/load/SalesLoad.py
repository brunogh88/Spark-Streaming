from pyspark.sql import functions as F
from importlib import import_module
from src.utils.config import config
from src.model.sales_struct import sales_struct

class SalesLoad(object):
    """
    Classe responsável por criar conexão com o tópico
    kafka com configurações da aplicação
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session


    def read_stream_sales(self):
        """
        Metodo responsável por abrir uma conexão com topico kafka
        e devolver somente o value do evento no formato dataframe

        :return: DataFrame do tipo stream
        """
        return (
            self.spark_session.readStream.format("kafka")
                .option("kafka.bootstrap.servers",config("KAFKA_BOOTSTRAP_SERVERS"))
                .option("subscribe", config("KAFKA_TOPIC"))
                .option("startingOffsets", "{\"teste\":{\"0\":141}}")
                .option("session.timeout.ms", config("KAFKA_SESSION_TIMEOUT_MS"))
                .option("heartbeat.interval.ms", config("KAFKA_HEARTBEAT_INTERVAL_MS"))
                .option("request.timeout.ms", config("KAFKA_REQUEST_TIMEOUT"))
                .option("connections.max.idle.ms", config("KAFKA_CONNECTION_MAX_IDLE_MS"))
                .option("failOnDataLoss", config("KAFKA_FAIL_ON_DATA_LOSS"))
                .option("fetchOffset.numRetries", config("KAFKA_NUMBER_RETRIES"))
                .option("fetchOffset.retryIntervalMs", config("KAFKA_RETRIES_INTERVAL_MS"))
                .option("maxOffsetsPerTrigger", config("KAFKA_MAX_OFFSETS_PER_TRIGGER"))
                .option("minBatchesToRetain", config("KAFKA_MIN_BATCHES_RETAIN"))
                .option("spark.hadoop.parquet.enable.summary-metadata", False)
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), sales_struct()).alias("all")) 
                .select("all.*")
        )

from src.enum.KafkaEnum import KafkaEnum

class KafkaUtils(object):
    """
        Classe que contém metodos para auxiliar no uso do kafka
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def readTopic(self):
        """
            Método para leitura do tópico SALES
        """
        return (
                self.spark_session.readStream.format(KafkaEnum().FORMAT)
                        .option("kafka.bootstrap.servers", KafkaEnum.BOOTSTRAP_SERVERS)
                        .option("subscribe", KafkaEnum.TOPIC)
                        .option("startingOffsets", "{\"sales\":{\"0\":2}}")
                        .option("session.timeout.ms", KafkaEnum.SESSION_TIMEOUT_MS)
                        .option("heartbeat.interval.ms", KafkaEnum.HEARTBEAT_INTERVAL_MS)
                        .option("request.timeout.ms", KafkaEnum.REQUEST_TIMEOUT)
                        .option("connections.max.idle.ms", KafkaEnum.CONNECTION_MAX_IDLE_MS)
                        .option("failOnDataLoss", KafkaEnum.FAIL_ON_DATA_LOSS)
                        .option("fetchOffset.numRetries", KafkaEnum.NUMBER_RETRIES)
                        .option("fetchOffset.retryIntervalMs", KafkaEnum.RETRIES_INTERVAL_MS)
                        .option("maxOffsetsPerTrigger", KafkaEnum.MAX_OFFSETS_PER_TRIGGER)
                        .option("minBatchesToRetain", KafkaEnum.MIN_BATCHES_RETAIN)
                        .option("spark.hadoop.parquet.enable.summary-metadata", False)
                        .load()
                        .selectExpr("CAST(value AS STRING)")
                )
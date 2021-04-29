from src.utils.KafkaUtils import KafkaUtils

class SalesLoad(object):
    """
    Classe responsável por retornar um DataFrame com os dados do topico Kafka
    """

    def __init__(self, spark_session):
        self.spark_session = spark_session


    def read_stream_sales(self):
        """
        Metodo responsável por deveolver um Dataframe com o schema Sales

        :return: DataFrame do tipo stream com o schema Sales
        """
        return KafkaUtils(self.spark_session).readTopic()

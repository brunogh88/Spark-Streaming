
class HdfsUtils(object):

    def __init__(self, spark_session):
        self.spark_session = spark_session

    def read(self, path, schema, format):
        return ( 
            self.spark_session
            .read
            .schema(schema)
            .format(format)
            .option("sep", ";")
            .option("header", "true")
            .load(path)
        )

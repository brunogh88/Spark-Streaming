

class HdfsUtils(object):

    def __init__(self, spark_session):
        self.spark_session = spark_session


    def read(self, path, schema, format):
        if(format == "csv"):
            return ( 
                self.spark_session
                .read
                .schema(schema)
                .format(format)
                .option("sep", ";")
                .option("header", "true")
                .load(path)
            )
        else:
            return ( 
                self.spark_session
                .read
                .schema(schema)
                .format(format)
                .load(path)
            )


    def writeStream(self, df, path, format, partition_name, save_mode, check_point_path, query_name):
        df.writeStream \
            .queryName(query_name) \
            .outputMode(save_mode) \
            .partitionBy(partition_name) \
            .format(format) \
            .option("path", path) \
            .option("truncate", False) \
            .option("checkpointLocation", check_point_path) \
            .start()

    def writeTriggerOnceStream(self, df, path, format, partition_name, save_mode, check_point_path, query_name):
        df.writeStream \
            .queryName(query_name) \
            .outputMode(save_mode) \
            .partitionBy(partition_name) \
            .format(format) \
            .option("path", path) \
            .option("truncate", False) \
            .option("checkpointLocation", check_point_path) \
            .trigger(once=True) \
            .start()

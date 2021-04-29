class SalesOracleIngest(object):

    def __init__(self):
        pass

    def __save_oracle(self, df, epoch_id):
        (
            df
            .write
            .format("jdbc")
            .option("batchsize", 1000)
            .mode("append")
            .jdbc("jdbc:oracle:thin:@localhost:1521/xe",
                  "TB_SALES",
                  properties={
                      "driver": "oracle.jdbc.OracleDriver",
                      "user": "DW",
                      "password": "test"
                      }
                  )
        )

    def save(self, df):
        (
            df
            .repartition(2)
            .writeStream
            .queryName("Sales-Oracle")
            .outputMode("Append")
            .format("jdbc")
            .foreachBatch(self.__save_oracle)
            .trigger(once=True)
            .start()
        )
        
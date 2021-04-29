from os import environ
from src.utils.spark_conf import get_spark_session
from src.pipe.SalesPipe import SalesPipe
from src.utils import log, arguments
from src.utils.timer import Timer
from src.utils.messages import APP_FINISHED, APP_STARTED, APP_ARGS

environ['PYSPARK_SUBMIT_ARGS'] = \
    """--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,
                  org.apache.kafka:kafka-clients:0.10.0.0
       --jars C:\\Projetos\\PySpark\\Spark-Streaming\\jar\\ojdbc6.jar
       pyspark-shell"""

def main(args, timer):
    try:
        timer.start()
        log.info(APP_STARTED)
        log.info(APP_ARGS.format(args))
        spark = get_spark_session(args.pipe)
        separator = ", "
        pipes = {
            "SALES": SalesPipe(spark, args)
        }
        pipe_run = pipes.get(args.pipe)
        if not pipe_run:
            list_names = separator.join(pipes.keys())
            raise Exception(
                "Pipe: '"+args.pipe+"', does not exist.  \
                The pipe valid is: ("+list_names+")"
            )

        pipe_run.start()
        spark.streams.awaitAnyTermination()
        spark.stop()
        log.info(APP_FINISHED.format(timer.end()))
    except Exception as exeption:
        log.error(str(exeption))
        raise


if __name__ == '__main__':
    main(arguments.get_args(), Timer())

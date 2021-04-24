from os import environ
import sys
from src.utils.spark_conf import get_spark_session
from src.pipe.SalesPipe import SalesPipe
from src.utils import log, arguments
from src.utils.timer import Timer
from src.utils.messages import APP_FINISHED, APP_STARTED, __title


environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:0.10.0.0 pyspark-shell"

def main(args, timer):
    try:
        timer.start()
        log.info(APP_STARTED)
        log.info("Pipe parameters: data -> {}, Pipe -> {}".format(args.date, args.pipe))
        spark = get_spark_session(args.pipe)
        separator = ", "
        pipes = {
            "SALES": SalesPipe(spark, sys.argv)
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
        log.info(APP_FINISHED.format(timer.end()))
    except Exception as e:
        log.error(str(e))
        raise


if __name__ == '__main__':
    main(arguments.get_args(), Timer())

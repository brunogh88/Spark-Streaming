from os import environ
import sys
from importlib import import_module

from src.utils.spark_conf import get_spark_session
from src.pipe.SalesPipe import SalesPipe
from src.utils import log

environ['PYSPARK_SUBMIT_ARGS'] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1,org.apache.kafka:kafka-clients:0.10.0.0 pyspark-shell"

def main():
    try:
        parameter_pipe = sys.argv[1]
        log.info("Pipe parameter: {}".format(parameter_pipe))
        spark = get_spark_session(parameter_pipe)
        separator = ", "
        pipes = {
            "SALES": SalesPipe(spark)
        }
        pipe_run = pipes.get(parameter_pipe)
        if not pipe_run:
            list_names = separator.join(pipes.keys())
            raise Exception(
                "Pipe: '"+parameter_pipe+"', não está implementada.  \
                As pipes implementadas para uso são: ("+list_names+")"
            )
        pipe_run.start()
    except IndexError:
        print("Error: Chamada sem parametros!")
        log.error("You need to send the parameters!")
    except Exception as e:
        log.error(str(e))
        raise


if __name__ == '__main__':
    main()

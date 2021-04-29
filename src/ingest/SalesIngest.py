from src.utils.hdfs_utils import HdfsUtils
from src.utils.config import config
from src.utils import log

class SalesIngest(object):
    
    def __init__(self):
        pass

    def save(self, df):
        log.info("Gravando no Refined")
        HdfsUtils(None).writeStream(
            df=df, 
            path=config("SPARK_REFINED_PATH")+config("SALES_PATH"),
            format=config("SPARK_PARQUET_FORMAT"),
            partition_name="dt_partition",
            save_mode="Append",
            check_point_path=config("SPARK_REFINED_PATH") + config("SALES_PATH") + config("CHECKPOINT_PATH"),
            query_name="sales-save-refined"
            )

from ccnet_spark.pipeline import Pipeline, Config
import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("CCNETSpark")
    .config("spark.executor.memory", "100g")
    .config("spark.driver.memory", "32g")
    .config("spark.driver.maxResultSize", "32g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)

config = Config(
    isSample=False,
    n_segments=4,
    sampleRate=0.1,
    cache_dir="../../cached_data/",
    output_dir="../../cached_data/",
    fasttext_model_path='../../cc_net/bin/lid.bin', 
    lm_dir='../../cc_net/data/lm_sp', 
    cutoff_csv_path='../../cc_net/cc_net/data/cutoff.csv',
    dump="2019-18",
)
print(config)

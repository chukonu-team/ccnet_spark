import os
from pyspark.sql import functions as F

from .util import convert_to_absolute_path
def save_tmp(
    spark_df,
    cache_dir:str,
    hdfs_dir:str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
    use_hdfs:bool=False,
    hdfs_http_url:str="http://node0:9870",
    hdfs_hdfs_url:str="hdfs://node0:9898"
):
    if use_hdfs:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(hdfs_dir,"hdfs_tmp_sdf_parquet",dump,saved_sdf_name)
        spark_df.write.mode("overwrite").parquet(f"{hdfs_hdfs_url}{saved_sdf_path}")
    else:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(cache_dir,"tmp_sdf_parquet",dump,saved_sdf_name)
        saved_sdf_path = convert_to_absolute_path(saved_sdf_path)

        os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
        spark_df.write.mode("overwrite").parquet(
            f"file:///{saved_sdf_path}"
        )
def save_partation(
    spark_df,
    cache_dir: str,
    hdfs_dir:str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
    use_hdfs:bool=False,
    hdfs_http_url:str="http://node0:9870",
    hdfs_hdfs_url:str="hdfs://node0:9898"
):
    if use_hdfs:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(hdfs_dir,"hdfs_result_sdf_parquet",dump,saved_sdf_name)
        spark_df.write.mode("overwrite").partitionBy("lang", "bucket").parquet(f"{hdfs_hdfs_url}{saved_sdf_path}")
    else:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(cache_dir,"result_sdf_parquet",dump,saved_sdf_name)
        saved_sdf_path = convert_to_absolute_path(saved_sdf_path)

        os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
        spark_df.write.mode("overwrite").partitionBy("lang", "bucket").parquet(
            f"file:///{saved_sdf_path}"
        )
def load_partation(
    spark,
    lang,
    bucket,
    cache_dir: str,
    hdfs_dir:str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
    use_hdfs:bool=False,
    hdfs_http_url:str="http://node0:9870",
    hdfs_hdfs_url:str="hdfs://node0:9898"
):
    if use_hdfs:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(hdfs_dir,"hdfs_result_sdf_parquet",dump,saved_sdf_name)
        df = spark.read.parquet(f"{hdfs_hdfs_url}{saved_sdf_path}/lang={lang}/bucket={bucket}")
    else:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(cache_dir,"result_sdf_parquet",dump,saved_sdf_name)
        saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
        if not os.path.exists(saved_sdf_path):
            os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
        df = spark.read.parquet(f"file:///{saved_sdf_path}/lang={lang}/bucket={bucket}")
    return df
def load_all(
    spark,
    cache_dir: str,
    hdfs_dir:str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
    use_hdfs:bool=False,
    hdfs_http_url:str="http://node0:9870",
    hdfs_hdfs_url:str="hdfs://node0:9898"
):
    if use_hdfs:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(hdfs_dir,"hdfs_result_sdf_parquet",dump,saved_sdf_name)
        df = spark.read.parquet(f"{hdfs_hdfs_url}{saved_sdf_path}")
    else:
        saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
        saved_sdf_path = os.path.join(cache_dir,"result_sdf_parquet",dump,saved_sdf_name)
        saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
        if not os.path.exists(saved_sdf_path):
            os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
        df = spark.read.parquet(f"file:///{saved_sdf_path}")
    return df
def analy_df(df):
    # 定义聚合函数求和
    sum_columns = [
        F.sum('original_length').alias('sum_original_length'),
        F.sum('length').alias('sum_length'),
        F.sum('nlines').alias('sum_nlines'),
        F.sum('original_nlines').alias('sum_original_nlines')
    ]
    # 按照 'bucket' 和 'lang' 字段进行分组，并计算每个组合的数量和求和
    aggregated_df = df.groupBy('bucket', 'lang').agg(
        F.count('*').alias('count'), *sum_columns
    )
    # 添加两列做比较
    aggregated_df = aggregated_df.withColumn('length_ratio', F.col('sum_length') / F.col('sum_original_length'))
    aggregated_df = aggregated_df.withColumn('nlines_ratio', F.col('sum_nlines') / F.col('sum_original_nlines'))

    # 显示聚合后的结果
    aggregated_df.show()
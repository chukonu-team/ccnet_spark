from ccnet_spark.pipe_save import convert_to_absolute_path
from pyspark.sql import SparkSession
import os
import pyarrow as pa
import pyarrow.parquet as pq

spark = (
    SparkSession.builder.appName("ccnetspark_profile_profile")
    .master("local[*]")
    .config("spark.executor.memory", "60g")
    .config("spark.driver.memory", "60g")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.driver.maxResultSize", "60g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    # .config("spark.sql.autoBroadcastJoinThreshold","-1")
    # .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    # .config("spark.driver.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    .getOrCreate()
)
def get_result(path_name):
    isSample=True
    min_len=300
    sampleRate=1
    dump="2019-18"
    save_type="file"
    output_dir="/metadata0/wxl_data/cached_data/"
    saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
    saved_sdf_path = os.path.join(output_dir,path_name,dump,saved_sdf_name)
    saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
    df = spark.read.parquet(f"{save_type}:///{saved_sdf_path}")
    return df
def save2pdf(path_name):
    df=get_result(path_name=path_name)
    pdf = df.toPandas()
    table = pa.Table.from_pandas(pdf)
    # 将 PyArrow 表保存为 Parquet 文件
    pq.write_table(table, f'/metadata0/wxl_data/{path_name}.parquet')
save2pdf("result_sdf_parquet_nokeep")
save2pdf("result_sdf_parquet_keep_dup")
from ccnet_spark.pipe_line import Pipeline, Config, PipelineStep
import time
from pyspark.sql import SparkSession
import sys
import subprocess
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
def getPIP(index):
    pips = [
        [],
        [
            PipelineStep.REAL_LEN,
        ],
        [PipelineStep.REAL_LEN, PipelineStep.HASH],
        [PipelineStep.REAL_LEN, PipelineStep.HASH, PipelineStep.DEDUP_KEEP],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
            PipelineStep.SP,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
            PipelineStep.SP,
            PipelineStep.LM,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
            PipelineStep.SP,
            PipelineStep.LM,
            PipelineStep.PP_BUCKET,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
            PipelineStep.SP,
            PipelineStep.LM,
            PipelineStep.PP_BUCKET,
            PipelineStep.DROP,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_KEEP,
            PipelineStep.LID,
            PipelineStep.SP,
            PipelineStep.LM,
            PipelineStep.PP_BUCKET,
            PipelineStep.UNKNOWN,
        ],
        [
            PipelineStep.REAL_LEN,
            PipelineStep.HASH,
            PipelineStep.DEDUP_NOKEEP,
            PipelineStep.LID,
            PipelineStep.SP,
            PipelineStep.LM,
            PipelineStep.PP_BUCKET,
            PipelineStep.DROP,
        ],
    ]
    return pips[index]

spark = (
    SparkSession.builder.appName("ccnetspark_local_profile_n")
    .master("local[*]")
    .config("spark.executor.memory", "10g")
    .config("spark.driver.memory", "10g")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.driver.maxResultSize", "5g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    # .config("spark.sql.autoBroadcastJoinThreshold","-1")
    # .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    # .config("spark.driver.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    .getOrCreate()
)

if __name__ == "__main__":
    # 从命令行参数中获取索引
    index = int(sys.argv[1])
    convert2pdf_path = str(sys.argv[2])
    pip = getPIP(index)
    print(f"pipline is:{pip}")
    # 执行命令并将输出重定向到文件
    subprocess.run(["bash", "./io_snapshot.sh"], stdout=open("./old_nvme.log", "w"))


    config = Config(
        isSample=False,
        n_segments=1,
        sampleRate=0.01,
        cache_dir="./cached_data/",
        fasttext_model_path="./cached_data/lid.bin",
        lm_dir="./cached_data/lm_sp",
        cutoff_csv_path="./cutoff.csv",
        dump="2019-18",
        pipeline=pip,
        use_hdfs=False,
        repartation_count=32,
        repartation_lang_count=32,
    )

    pipeline = Pipeline(config, spark)
    s = time.time()
    df = pipeline.load_data()
    pipeline.run_pipeline()
    # pipeline.timer()
    # pipeline.save_to_tmp()
    pipeline.save_data()
    e = time.time()
    print("==============================================")
    print(f"pipeline:{[i.value for i in pip]}, time consume:{round(e-s,3)}s")
    subprocess.run(["bash", "./io_snapshot.sh"], stdout=open("./new_nvme.log", "w"))
    subprocess.run(["bash", "./io_diff.sh"])

    if(convert2pdf_path!=""):
        print("start convert df to pdf")
        df=pipeline.load_result_data()
        pdf = df.toPandas()
        table = pa.Table.from_pandas(pdf)
        # 将 PyArrow 表保存为 Parquet 文件
        pq.write_table(table, f'{convert2pdf_path}')
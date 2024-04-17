from ccnet_spark.pipe_line import Pipeline, Config, PipelineStep
import time
from pyspark.sql import SparkSession
import sys


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
    ]
    return pips[index]


spark = (
    SparkSession.builder.appName("ccnetspark_local_profile")
    .master("local[*]")
    .config("spark.executor.memory", "100g")
    .config("spark.driver.memory", "60g")
    .config("spark.driver.maxResultSize", "60g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.autoBroadcastJoinThreshold","-1")
    # .config("spark.executor.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    # .config("spark.driver.extraJavaOptions", "-XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintGCTimeStamps")
    .getOrCreate()
)

if __name__ == "__main__":
    # 从命令行参数中获取索引
    index = int(sys.argv[1])
    pip = getPIP(index)
    print(f"pipline is:{pip}")
    config = Config(
        isSample=False,
        n_segments=40,
        sampleRate=0.01,
        cache_dir="/metadata0/wxl_data/cached_data/",
        output_dir="/metadata0/wxl_data/cached_data/",
        fasttext_model_path="/metadata0/wxl_data/lid.bin",
        lm_dir="/metadata0/wxl_data/lm_sp",
        cutoff_csv_path="/metadata0/wxl_data/cutoff.csv",
        dump="2019-18",
        pipeline=pip,
    )

    pipeline = Pipeline(config, spark)
    s = time.time()
    df = pipeline.load_data()
    pipeline.run_pipeline()
    # random_row = pipeline.df.sample(fraction=0.00001, seed=42)
    # res=random_row.collect()
    # random_row = pipeline.df.orderBy(rand()).limit(1)
    # random_row = pipeline.df.rdd.takeSample(False, 1, seed=42)
    # pipeline.timer()
    # pipeline.save_to_tmp()
    # res=pipeline.df.select("url").rdd.count()
    pipeline.save_data()
    e = time.time()
    print("==============================================")
    print(f"pipeline:{[i.value for i in pip]}, time consume:{round(e-s,3)}s")

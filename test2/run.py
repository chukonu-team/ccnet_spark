from ccnet_spark.pipe_line import Pipeline, Config
import time
from pyspark.sql import SparkSession
pips = [
    [],
    [
        "real_len",
    ],
    [
        "real_len",
        "hash",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
        "pp_bucket",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
        "pp_bucket",
        "drop",
    ],
]


spark = (
    SparkSession.builder.appName("CCNETSpark")
    .config("spark.executor.memory", "100g")
    .config("spark.driver.memory", "32g")
    .config("spark.driver.maxResultSize", "32g")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .getOrCreate()
)
times = []
for p in pips:
    config = Config(
        isSample=False,
        n_segments=1,
        sampleRate=0.01,
        cache_dir="../../cached_data/",
        output_dir="../../cached_data/",
        fasttext_model_path='../../cc_net/bin/lid.bin', 
        lm_dir='../../cc_net/data/lm_sp', 
        cutoff_csv_path='../../cc_net/cc_net/data/cutoff.csv',
        dump="2019-18",
    )
    print(config)
    pipeline = Pipeline(config,spark)
    df = pipeline.load_data()
    s = time.time()
    pipeline.run()
    pipeline.df.count()
    e = time.time()
    times.append(e - s)
    print(f"config:{config} time consume:{e-s}")
print(f"times:{times}")

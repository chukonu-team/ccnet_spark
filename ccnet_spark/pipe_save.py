import os


def save_partation(
    spark_df,
    cache_folder: str,
    date: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    samplePath = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100))
    output_path = (
        cache_folder
        + "/cache_result_parquet/"
        + date
        + "/"
        + "minlen_"
        + str(min_len)
        + samplePath
        + ".parquet"
    )  # 设置输出路径
    if not os.path.exists(output_path):
        os.makedirs("/".join(output_path.split("/")[:-1]), exist_ok=True)
    spark_df.write.mode("overwrite").partitionBy("lang", "bucket").parquet(
        f"file:///{output_path}"
    )


def load_partation(
    spark,
    lang,
    bucket,
    cache_folder: str,
    date: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    samplePath = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100))
    output_path = (
        cache_folder
        + "/cache_result_parquet/"
        + date
        + "/"
        + "minlen_"
        + str(min_len)
        + samplePath
        + ".parquet"
    )  # 设置输出路径
    if not os.path.exists(output_path):
        os.makedirs("/".join(output_path.split("/")[:-1]), exist_ok=True)
    df = spark.read.parquet(f"file:///{output_path}/lang={lang}/bucket={bucket}")
    return df

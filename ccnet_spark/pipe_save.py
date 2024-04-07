import os

from .pipe_preprocess import convert_to_absolute_path
def save_partation(
    spark_df,
    output_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
    saved_sdf_path = os.path.join(output_dir,"sdf_parquet",dump,saved_sdf_name)
    saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
    if not os.path.exists(saved_sdf_path):
        os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
        spark_df.write.mode("overwrite").partitionBy("lang", "bucket").parquet(
            f"file:///{saved_sdf_path}"
        )


def load_partation(
    spark,
    lang,
    bucket,
    output_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
    saved_sdf_path = os.path.join(output_dir,"sdf_parquet",dump,saved_sdf_name)
    saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
    if not os.path.exists(saved_sdf_path):
        os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
    df = spark.read.parquet(f"file:///{saved_sdf_path}/lang={lang}/bucket={bucket}")
    return df
def load_all(
    spark,
    output_dir: str,
    dump: str = "2019-09",
    isSample: bool = False,
    sampleRate: float = 0.1,
    min_len: int = 300,
):
    saved_sdf_name = "_sampleRate_" + str(int(sampleRate*100 if isSample else 100)) + "_min_len_"+str(min_len)+".parquet"
    saved_sdf_path = os.path.join(output_dir,"sdf_parquet",dump,saved_sdf_name)
    saved_sdf_path = convert_to_absolute_path(saved_sdf_path)
    if not os.path.exists(saved_sdf_path):
        os.makedirs("/".join(saved_sdf_path.split("/")[:-1]), exist_ok=True)
    df = spark.read.parquet(f"file:///{saved_sdf_path}")
    return df
import argparse
import time
from pyspark.sql import SparkSession
from ccnet_spark.pipe_load import download_and_parse

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parallel File Download from CommonCrawl")
    parser.add_argument("--dump", required=True, help="The dump name to download, e.g., 2019-18")
    parser.add_argument("--cache_dir", required=True, help="The cache directory to store downloaded data")
    parser.add_argument("--root_dir", default="https://data.commoncrawl.org", help="The root directory URL of the data source")
    parser.add_argument("--segments", nargs='+', type=int, default=[i for i in range(10)], help="List of segments to download")

    args = parser.parse_args()

    t1 = time.time()

    segments = args.segments
    dump = args.dump
    cache_dir = args.cache_dir
    root_dir = args.root_dir

    # 创建 SparkSession
    spark = SparkSession.builder \
        .appName("Parallel File Download") \
        .config("spark.driver.memory", "50g") \
        .config("spark.driver.maxResultSize", "50g") \
        .getOrCreate()

    res = spark.sparkContext.parallelize(segments).flatMap(lambda segment: download_and_parse(segment, segments.index(segment), dump, cache_dir, root_dir))
    df = spark.createDataFrame(res)
    c = df.count()

    t2 = time.time()

    print("=====================================================================")
    print(f"timeconsume: download_and_parse: {t2 - t1}s, doc count: {c}; segments count:{len(segments)}")
    print("=====================================================================")
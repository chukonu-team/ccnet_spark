from pathlib import Path
from typing import NamedTuple, Sequence
from .pipe_preprocess import load_segments
from .pipe_hash import compute_hashes, split_doc2para
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import col
from .pipe_lid import predictLang
from .pipe_tokenized import doSentencePiece
from .pipe_perplexity import doDocLM
from .pipe_ppbucket import doPPBucket
from .pipe_save import save_partation,load_partation
DEFAULT_PIPELINE = [
    "real_len",
    "hash",
    "dedup_keep",  # "dedup_nokeep"
    "lid",
    "sp",
    "lm",
    "pp_bucket",
    "drop",
]


class Config(NamedTuple):
    """
    Mine Common Crawl with the given settings.
    n_segments=10
    cache_folder="/root/wxl_folder/cache_data/"
    date="2019-09" ## hardcode ,现在只能是这个
    segments=[i for i in range(n_segments)]
    min_len=300
    isSample=True
    sampleRate=0.01
    """

    dump: str = "2019-09"
    cache_dir: str = "../cache_data/"
    output_dir: str = "../cache_data/"
    min_len: int = 300
    isSample: bool = False
    sampleRate: float = 0.01
    n_segments: int = 10
    pipeline: Sequence[str] = DEFAULT_PIPELINE
    spark: SparkSession = (
        SparkSession.builder.appName("CCNETSpark")
        .config("spark.executor.memory", "100g")
        .config("spark.driver.memory", "32g")
        .config("spark.driver.maxResultSize", "32g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


class Pipeline:
    def __init__(self, config: Config):
        #### loaded from config
        self.dump = config.dump
        self.cache_dir = config.cache_dir
        self.output_dir= config.output_dir
        self.min_len = config.min_len
        self.isSample = config.isSample
        self.sampleRate = config.sampleRate
        self.n_segments = config.n_segments
        self.pipelines = config.pipeline
        self.spark = config.spark
        #### computed by config:
        self.segments = [i for i in range(self.n_segments)]

    def load_data(self):
        spark_df = load_segments(
            self.spark,
            self.segments,
            self.cache_dir,
            dump=self.dump,
            isSample=self.isSample,
            sampleRate=self.sampleRate,
            min_len=self.min_len,
        )
        self.df = spark_df
        return spark_df

    def run(self):
        for pipeline in self.pipelines:
            if pipeline == "real_len":
                self.df = self.df.withColumn("length", F.length(self.df["raw_content"]))
            elif pipeline == "hash":
                split_result = self.df.withColumn(
                    "split_content", split_doc2para(self.df["raw_content"])
                )
                exploded_df = split_result.withColumn(
                    "exploded_content", explode(split_result.split_content)
                ).drop("split_content")
                self.df = exploded_df.withColumn(
                    "hash_value", compute_hashes(exploded_df.exploded_content.raw_line)
                )
            elif pipeline == "dedup_keep" or pipeline == "dedup_nokeep":
                if pipeline == "dedup_keep":
                    self.df = self.df.dropDuplicates(
                        ["hash_value"]
                    )  # 第一种是保留一次重复行
                else:
                    duplicate_counts = (
                        self.df.groupBy("hash_value").count().where(col("count") > 1)
                    )
                    # 根据重复行的信息，使用 filter 过滤掉重复行
                    self.df = self.df.join(
                        duplicate_counts, on="hash_value", how="left_anti"
                    )

                group_df = self.df.groupBy("digest").agg(
                    F.first("url").alias("url"),
                    F.first("date_download").alias("date_download"),
                    F.first("source_domain").alias("source_domain"),
                    F.first("cc_segment").alias("cc_segment"),
                    F.first("length").alias("original_length"),
                    F.first("nlines").alias("original_nlines"),
                    F.first("title").alias("title"),
                    F.count("exploded_content.raw_line_id").alias("nlines"),
                    F.sort_array(F.collect_list("exploded_content")).alias(
                        "exploded_content"
                    ),
                )
                group_df = group_df.withColumn(
                    "raw_content", F.concat_ws("\n", "exploded_content.raw_line")
                )
                group_df = group_df.withColumn(
                    "raw_line_id", group_df.exploded_content.raw_line_id
                )
                self.df = group_df.withColumn("length", F.length("raw_content")).drop(
                    "exploded_content"
                )
            elif pipeline == "lid":
                lang_df = group_df.withColumn("lang_score", predictLang("raw_content"))
                self.df = lang_df.withColumn("lang", lang_df.lang_score.lang) \
                                .withColumn("score", lang_df.lang_score.score) \
                                .drop("lang_score")
            elif pipeline == "sp":
                self.df = self.df.withColumn("tokenized", doSentencePiece("raw_content","lang"))
            elif pipeline == "lm":
                self.df = self.df.withColumn("perplexity", doDocLM("tokenized","lang"))
            elif pipeline == "pp_bucket":
                self.df = self.df.withColumn("bucket", doPPBucket("perplexity","lang"))
            elif pipeline == "drop":
                self.df = self.df.drop("tokenized")
    def save_data(self):
        save_partation(self.df,self.output_dir,self.dump,self.isSample,self.sampleRate,self.min_len)
    def load_partation_data(self,lang,bucket):
        return load_partation(self.spark,lang,bucket,self.output_dir,self.dump,self.isSample,self.sampleRate,self.min_len)
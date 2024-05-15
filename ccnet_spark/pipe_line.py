from typing import NamedTuple, Sequence

from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import col

from .pipe_lid import predictLang
from .pipe_preprocess import load_segments
from .pipe_hash import compute_hashes, split_doc2para
from .pipe_tokenized import doSentencePiece
from .pipe_perplexity import doDocLM
from .pipe_ppbucket import doPPBucket
from .pipe_save import save_partation, load_partation,load_all, analy_df, save_tmp
import pandas as pd
from enum import Enum

class PipelineStep(Enum):
    REAL_LEN = "real_len"
    HASH = "hash"
    DEDUP_KEEP = "dedup_keep"
    DEDUP_NOKEEP = "dedup_nokeep"
    LID = "lid"
    SP = "sp"
    LM = "lm"
    PP_BUCKET = "pp_bucket"
    DROP = "drop"
    UNKNOWN = "unknown"


DEFAULT_PIPELINE = [
    PipelineStep.REAL_LEN,
    PipelineStep.HASH,
    PipelineStep.DEDUP_KEEP,
    PipelineStep.LID,
    PipelineStep.SP,
    PipelineStep.LM,
    PipelineStep.PP_BUCKET,
    PipelineStep.DROP,
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
    hdfs_dir:str = "/data0/k8s/node0_data/ccnet_spark/cached_data/"
    min_len: int = 300
    isSample: bool = False
    sampleRate: float = 0.01
    n_segments: int = 10
    pipeline: Sequence[PipelineStep] = DEFAULT_PIPELINE
    threshold: float = 0.5
    fasttext_model_path: str = "../cc_net/bin/lid.bin"
    lm_dir: str = "../cc_net/data/lm_sp"
    cutoff_csv_path: str = "../cc_net/cc_net/" + "data/" + "cutoff.csv"
    percentile_head: int = 30
    percentile_tail: int = 60
    use_hdfs: bool = False
    hdfs_http_url:str="http://node0:9870"
    hdfs_hdfs_url:str="hdfs://node0:9898"
    repartation_count:int=0

class Pipeline:
    def __init__(self, config: Config, spark: SparkSession):
        #### loaded from config
        self.dump = config.dump
        self.cache_dir = config.cache_dir
        self.output_dir = config.output_dir
        self.min_len = config.min_len
        self.isSample = config.isSample
        self.sampleRate = config.sampleRate
        self.n_segments = config.n_segments
        self.pipelines = config.pipeline
        self.threshold = config.threshold
        self.lm_dir = config.lm_dir
        self.fasttext_model_path = config.fasttext_model_path
        self.cutoff_csv_path = config.cutoff_csv_path
        self.percentile_head = config.percentile_head
        self.percentile_tail = config.percentile_tail
        self.spark = spark
        self.use_hdfs = config.use_hdfs
        self.hdfs_dir= config.hdfs_dir
        self.hdfs_hdfs_url=config.hdfs_hdfs_url
        self.hdfs_http_url=config.hdfs_http_url
        self.repartation_count=config.repartation_count
        #### computed by config:
        self.segments = [i for i in range(self.n_segments)]
        cutoffs = pd.read_csv(self.cutoff_csv_path, index_col=0)
        self.cutoffs = {
            lang: (
                cutoffs[lang][self.percentile_head],
                cutoffs[lang][self.percentile_tail],
            )
            for lang in cutoffs.columns
        }
    def timer(self):
        n=len(self.pipelines)
        if(n==0):
            _=self.df.select("length").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.REAL_LEN):
            _=self.df.select("length").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.HASH):
            _=self.df.withColumn("tmp_len", F.length(self.df["hash_value"])).select('tmp_len').rdd.count()
        elif (self.pipelines[-1] == PipelineStep.DEDUP_KEEP or self.pipelines[-1]== PipelineStep.DEDUP_NOKEEP):
            self.df=self.df.withColumn("raw_content_len", F.length(self.df["raw_content"])).withColumn("raw_line_id_len", F.size(self.df["raw_line_id"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines',"raw_content_len").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.LID):
            self.df=self.df.withColumn("raw_line_id_len", F.size(self.df["raw_line_id"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines','lang','score').rdd.count()
        elif (self.pipelines[-1] == PipelineStep.SP):
            self.df=self.df.withColumn("raw_line_id_len", F.size(self.df["raw_line_id"])).withColumn("tokenized_len", F.length(self.df["tokenized"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines','lang','score',"tokenized_len").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.LM):
            self.df=self.df.withColumn("raw_line_id_len", F.size(self.df["raw_line_id"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines','lang','score',"perplexity").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.PP_BUCKET):
            self.df=self.df.withColumn("raw_line_id_len", F.size(self.df["raw_line_id"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines','lang','score',"bucket").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.DROP):
            self.df=self.df.withColumn("raw_line_id_len", F.size(self.df["raw_line_id"]))
            _=self.df.select("length","nlines",'raw_line_id_len','original_length','original_nlines','lang','score',"bucket").rdd.count()
        elif (self.pipelines[-1] == PipelineStep.UNKNOWN):
            _=self.df.rdd.count()
        else:
            print("unknown pipeline")
    def load_data(self):
        spark_df = load_segments(
            self.spark,
            self.segments,
            self.cache_dir,
            dump=self.dump,
            isSample=self.isSample,
            sampleRate=self.sampleRate,
            min_len=self.min_len,
            use_hdfs=self.use_hdfs,
            hdfs_hdfs_url=self.hdfs_hdfs_url,
            hdfs_http_url=self.hdfs_http_url,
            hdfs_dir=self.hdfs_dir
        )
        self.origin_df = spark_df
        self.df = spark_df
        return spark_df

    def run_step(self, pipeline: PipelineStep):
        if not isinstance(pipeline, PipelineStep):
            raise ValueError("Invalid pipeline type. Expected Pipeline enum member.")
        if pipeline == PipelineStep.REAL_LEN:
            self.df = self.df.withColumn("length", F.length(self.df["raw_content"]))
        elif pipeline == PipelineStep.HASH:
            self.compute_hashes()
        elif (
            pipeline == PipelineStep.DEDUP_KEEP or pipeline == PipelineStep.DEDUP_NOKEEP
        ):
            self.deduplicate(pipeline)
        elif pipeline == PipelineStep.LID:
            self.predict_lang()
        elif pipeline == PipelineStep.SP:
            self.do_sentence_piece()
        elif pipeline == PipelineStep.LM:
            self.do_doc_lm()
        elif pipeline == PipelineStep.PP_BUCKET:
            self.do_pp_bucket()
        elif pipeline == PipelineStep.DROP:
            self.drop_columns()

    def compute_hashes(self):
        split_result = self.df.withColumn(
            "split_content", split_doc2para(self.df["raw_content"])
        )
        exploded_df = split_result.withColumn(
            "exploded_content", explode(split_result.split_content)
        ).drop("split_content")
        self.df = exploded_df.withColumn(
            "hash_value", compute_hashes(exploded_df.exploded_content.raw_line)
        )

    def deduplicate(self, pipeline: PipelineStep):
        if pipeline == PipelineStep.DEDUP_KEEP:
            self.df = self.df.dropDuplicates(["hash_value"])
        else:
            duplicate_counts = (
                self.df.groupBy("hash_value").count().where(col("count") > 1)
            )
            self.df = self.df.join(duplicate_counts, on="hash_value", how="left_anti")
        self.group_and_concat()

    def group_and_concat(self):
        group_df = self.df.groupBy("digest").agg(
            F.first("url").alias("url"),
            F.first("date_download").alias("date_download"),
            F.first("source_domain").alias("source_domain"),
            F.first("cc_segment").alias("cc_segment"),
            F.first("length").alias("original_length"),
            F.first("nlines").alias("original_nlines"),
            F.first("title").alias("title"),
            F.count("exploded_content.raw_line_id").alias("nlines"),
            F.sort_array(F.collect_list("exploded_content")).alias("exploded_content"),
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

    def predict_lang(self):
        lang_df = self.df.withColumn(
            "lang_score",
            predictLang(
                "raw_content",
                F.lit(self.fasttext_model_path),
                F.lit(self.threshold),
            ),
        )
        self.df = (
            lang_df.withColumn("lang", lang_df.lang_score.lang)
            .withColumn("score", lang_df.lang_score.score)
            .drop("lang_score")
        )
        if(self.repartation_count>0):
            self.df = self.df.repartition("lang").repartition(self.repartation_count)
        
        # self.df = self.df.repartitionByRange(6, "lang")


    def do_sentence_piece(self):
        self.df = self.df.withColumn(
            "tokenized",
            doSentencePiece("raw_content", "lang", F.lit(self.lm_dir)),
        )

    def do_doc_lm(self):
        self.df = self.df.withColumn(
            "perplexity", doDocLM("tokenized", "lang", F.lit(self.lm_dir))
        )

    def do_pp_bucket(self):
        self.df = self.df.withColumn(
            "bucket", doPPBucket("perplexity", "lang", F.lit(str(self.cutoffs)))
        )

    def drop_columns(self):
        self.df = self.df.drop("tokenized")

    def run_pipeline(self):
        for pipeline in self.pipelines:
            self.run_step(pipeline)

    def save_to_tmp(self):
        save_tmp(
            self.df,
            self.output_dir,
            self.dump,
            self.isSample,
            self.sampleRate,
            self.min_len,
            use_hdfs=self.use_hdfs,
            hdfs_hdfs_url=self.hdfs_hdfs_url,
            hdfs_http_url=self.hdfs_http_url,
            hdfs_dir=self.hdfs_dir
        )

    def save_data(self):
        save_partation(
            self.df,
            self.output_dir,
            self.dump,
            self.isSample,
            self.sampleRate,
            self.min_len,
            use_hdfs=self.use_hdfs,
            hdfs_hdfs_url=self.hdfs_hdfs_url,
            hdfs_http_url=self.hdfs_http_url,
            hdfs_dir=self.hdfs_dir
        )

    def load_partation_data(self, lang, bucket):
        return load_partation(
            self.spark,
            lang,
            bucket,
            self.output_dir,
            self.dump,
            self.isSample,
            self.sampleRate,
            self.min_len,
            use_hdfs=self.use_hdfs,
            hdfs_hdfs_url=self.hdfs_hdfs_url,
            hdfs_http_url=self.hdfs_http_url,
            hdfs_dir=self.hdfs_dir
        )
    def load_result_data(self):
        return load_all(
            self.spark,
            self.output_dir,
            self.dump,
            self.isSample,
            self.sampleRate,
            self.min_len,
            use_hdfs=self.use_hdfs,
            hdfs_hdfs_url=self.hdfs_hdfs_url,
            hdfs_http_url=self.hdfs_http_url,
            hdfs_dir=self.hdfs_dir
        )

    def analy(self):
        analy_df(self.df)

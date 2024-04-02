from ccnet_spark.text_normalizer import normalize
from ccnet_spark.pipe_preprocess import load_segments
from ccnet_spark.pipe_hash import compute_hashes,split_doc2para
from ccnet_spark.pipe_lid import predictLang,predictScore
from ccnet_spark.pipe_tokenized import doSentencePiece
from ccnet_spark.pipe_perplexity import doDocLM
from ccnet_spark.pipe_ppbucket import doPPBucket
from ccnet_spark.pipe_save import save_partation,load_partation
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import explode
from pyspark.sql.functions import sum as spark_sum

# 初始化 SparkSession
spark = SparkSession.builder.appName("CCNETSpark")  \
                    .config("spark.executor.memory", "100g") \
                    .config("spark.driver.memory", "32g") \
                    .config("spark.driver.maxResultSize", "32g") \
                    .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
                    .getOrCreate()
def getModePara(mode):
    if(mode=="test"):
        cache_folder="/root/wxl_folder/cache_data/"
        date="2019-09" ## hardcode ,现在只能是这个
        segments=[i for i in range(10)]
        min_len=300
        isSample=True
        sampleRate=0.01
    else:
        cache_folder="/root/wxl_folder/cache_data/"
        date="2019-09" ## hardcode ,现在只能是这个
        segments=[i for i in range(10)]
        min_len=300
        isSample=True
        sampleRate=0.01
    return [cache_folder,date,segments,min_len,isSample,sampleRate]
mode="test"
cache_folder,date,segments,min_len,isSample,sampleRate=getModePara(mode)
spark_df=load_segments(spark,segments,cache_folder,date=date,isSample=isSample,sampleRate=sampleRate,min_len=min_len)
spark_df=spark_df.withColumn("length", F.length(spark_df["raw_content"]))
split_result = spark_df.withColumn("split_content", split_doc2para(spark_df["raw_content"]))
exploded_df=split_result.withColumn("exploded_content", explode(split_result.split_content))
exploded_df = exploded_df.withColumn("raw_line_id", exploded_df.exploded_content.raw_line_id) \
                         .withColumn("raw_line", exploded_df.exploded_content.raw_line) \
                         .drop("exploded_content")
hash_df = exploded_df.withColumn("hash_value", compute_hashes(exploded_df.raw_line))
deduplicated_df = hash_df.dropDuplicates(['hash_value'])
group_df = deduplicated_df.groupBy("digest").agg(
    F.first("url").alias("url"),
    F.first("date_download").alias("date_download"),
    F.first("source_domain").alias("source_domain"),
    F.first("cc_segment").alias("cc_segment"),
    F.first("length").alias("original_length"),
    F.first("nlines").alias("original_nlines"),
    F.first("title").alias("title"),
    F.concat_ws("\n", F.collect_list("raw_line").alias("raw_content")).alias("raw_content"),
    F.count("raw_line_id").alias("nlines"),
    F.collect_list("raw_line_id").alias("line_ids"),
)
group_df=group_df.withColumn("length", F.length(group_df["raw_content"]))
lang_df = group_df.withColumn("lang", predictLang("raw_content"))
lang_df = lang_df.withColumn("score", predictScore("raw_content"))
lm_df = lang_df.withColumn("tokenized", doSentencePiece("raw_content","lang"))
doclm_df = lm_df.withColumn("perplexity", doDocLM("tokenized","lang"))
bucket_df = doclm_df.withColumn("bucket", doPPBucket("perplexity","lang"))
drop_df = bucket_df.drop("tokenized")
save_partation(drop_df,cache_folder,date,isSample,sampleRate,min_len)
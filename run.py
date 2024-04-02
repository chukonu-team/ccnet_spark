# ccnet spark pipeline 实现

## 1. 导入依赖



from ccnet_spark import open_read, parse_warc_file,compute_hashes,NaiveHashSet, text_normalizer
from pathlib import Path
import numpy as np
import time
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType,IntegerType,StructType, StructField
from pyspark.sql import functions as F
from pyspark.sql.functions import udf, explode
from pyspark.sql.functions import sum as spark_sum
from cachetools import cached ### model 缓存

# 初始化 SparkSession
spark = SparkSession.builder.appName("CCNETSpark")  \
                    .config("spark.executor.memory", "100g") \
                    .config("spark.driver.memory", "32g") \
                    .config("spark.driver.maxResultSize", "32g") \
                    .config('spark.sql.execution.arrow.pyspark.enabled', 'true') \
                    .getOrCreate()
sc = spark.sparkContext


## 2. 读取文件数据，处理成pandas DataFrame

### 2.1 获取cache文件路径



cache_data="../cache_data/2019-09/"
def getWETURL(segment: int):
    cache_file_prefix = "CC-MAIN-20190215183319-20190215205319-"
    cache_file_sufix = ".warc.wet.gz"
    segment_str = str(segment).zfill(5)  # Pad with leading zeros
    return cache_data+cache_file_prefix + segment_str + cache_file_sufix
url = getWETURL(3)


### 2.2 处理文件，存入pandas DataFrame



def getpdf(segment,isPart:bool):
    file_path=Path(getWETURL(segment))
    file=open_read(file_path)
    s=time.time()
    pandas_df = parse_warc_file(file, 30)
    if(isPart):
        random_save_n=100
        pandas_df = pandas_df.sample(n=random_save_n, random_state=1)
    e=time.time()
    print(f"====== parse segment:{segment} to pd_df consume:{e-s} s")
    return pandas_df


## 3. 读取 spark dataframe 文件



def getsdf(segment,isPart:bool):
    inner_path = "_part" if isPart else "_all"
    output_path = cache_data+"cache_parquet/"+str(segment)+  inner_path +".parquet"  # 设置输出路径
    # 检查本地文件是否存在
    if not os.path.exists(output_path):
        print(f"======process to parquet of segment {segment}{inner_path}")
        # 处理文件并生成 Spark DataFrame
        pdf = getpdf(segment,isPart=isPart)
        pdf.to_parquet(output_path)  # 保存为 parquet 文件
        spark_df = spark.createDataFrame(pdf)
    else:
        print(f"======read parquet of segment {segment}{inner_path} from cache")
        pdf = pd.read_parquet(output_path)
        spark_df = spark.createDataFrame(pdf)
    return spark_df
def getsdfs(segments,isPart:bool = False):
    merged_sdf=None
    for seg in segments:
        if(merged_sdf):
            merged_sdf = merged_sdf.unionAll(getsdf(seg,isPart)) # Merge DataFrames
        else:
            merged_sdf = getsdf(seg,isPart)
    return merged_sdf


### 3.1 load spark DataFrame



def getModePara(mode):
    if(mode=="test"):
        para={
            "isTest":True,
            "isPart":True,
            "segments":5,
        }
        return para
    else:
        para={
            "isTest":False,
            "isPart":False,
            "segments":10,
        }
        return para




mode="dev"
mode_para=getModePara(mode)
segments=[i for i in range(mode_para["segments"])]
isPart=mode_para["isPart"]




s=time.time()
spark_df = getsdfs(segments,isPart=isPart)
num_docs=spark_df.count()
e=time.time()
print(f"load {len(segments)} segments,with {num_docs} docs,comsume:{e-s}s")

### 3.2 字段分析
# 1. wet 文件本身带有长度："length": length,这个是从wet的"Content-Length:"读出来的，和我计算len(raw_content）有出入。考虑原因是原先的length不只是说raw_content，还包括title等。



if(mode_para["isTest"]):
    print("=== TestMode Log:")
    s=time.time()
    print(spark_df.summary())
    tmp_df = spark_df.withColumn("compute_length", F.length(spark_df["raw_content"]))
    tmp_df.select("url","length","nlines","compute_length").show(5)
    e=time.time()
    print(f"time consume:{e-s}s")

### 3.3 修改length



spark_df=spark_df.withColumn("length", F.length(spark_df["raw_content"]))


## 4. hash计算

### 4.1 定义UDF,将doc 分割成paragraph 



# 定义一个函数，用于分割文本
def split_raw_content(content):
    lines = content.split('\n')
    line_ids = range(0, len(lines))  # 生成行号
    return list(zip(line_ids, lines))

# 注册为UDF
split_udf = udf(split_raw_content, ArrayType(StructType([
    StructField("raw_line_id", IntegerType(), False),
    StructField("raw_line", StringType(), False)
])))


### 4.2 udf 处理添加新字段



# 假设spark_df是您的DataFrame
# 使用UDF对raw_content字段进行处理
split_result = spark_df.withColumn("split_content", split_udf(spark_df["raw_content"]))
if(mode_para["isTest"]):
    print("=== TestMode Log:")
    s=time.time()
    print(split_result.summary())
    split_result.select("url","length","nlines","raw_content","split_content").show(5)
    e=time.time()
    print(f"time consume:{e-s}s")


### 4.3 将新字段展开获取paragraph级别row



# Explode the split_content column and select the desired columns
exploded_df = split_result.select("url","date_download","digest","length","nlines","source_domain","title","raw_content", explode(split_result.split_content).alias("exploded_content"))

# Split the exploded_content struct into separate columns
exploded_df = exploded_df.withColumn("raw_line_id", exploded_df.exploded_content.raw_line_id)
exploded_df = exploded_df.withColumn("raw_line", exploded_df.exploded_content.raw_line)

# Drop the exploded_content column if needed
exploded_df = exploded_df.drop("exploded_content")

if(mode_para["isTest"]):
    exploded_df.cache()
    print("=== TestMode Log:")
    s=time.time()
    print(exploded_df.summary())
    exploded_df.select("url","raw_content","raw_line_id","raw_line").show(5)
    e=time.time()
    print(f"time consume:{e-s}s")


### 4.4 添加hash 列



import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from ccnet_spark import normalize_for_dedup
from typing import Iterable, Iterator, Sequence, Sized, Tuple, Type
HASH_TYPE: Type[np.uint64] = np.uint64
HASH_SIZE = HASH_TYPE(0).nbytes 
print(f"HASH_SIZE:{HASH_SIZE}") # 8 Byte ==> 64bit
@udf(returnType=BinaryType())
def compute_hashes(line):
    if not line:
        return None
    normalized_line = normalize_for_dedup(line)  # Assuming normalize_for_dedup is defined
    line_hash = hashlib.sha1(bytes(normalized_line, encoding="utf-8")).digest()[:HASH_SIZE]
    return line_hash

# Assuming you have a dataframe named 'df' with a 'raw_line' column
hash_df = exploded_df.withColumn("hash_value", compute_hashes(exploded_df.raw_line))

# Show the resulting dataframe
if(mode_para["isTest"]):
    print("=== TestMode Log:")
    s=time.time()
    print(hash_df.summary())
    hash_df.show(5)
    e=time.time()
    print(f"time consume:{e-s}s")

### 4.5根据 hash 去重



deduplicated_df = hash_df.dropDuplicates(['hash_value'])
# Show the resulting dataframe
if(mode_para["isTest"]):
    print("=== TestMode Log:")
    deduplicated_df.cache()
    s=time.time()
    print(deduplicated_df.summary())
    deduplicated_df.select("url","length","nlines","raw_content","raw_line_id","hash_value").show(5)
    e=time.time()
    print(f"time consume:{e-s}s")

                                                                                    

### 4.6 聚合
# 将段落重新聚合为doc



from pyspark.sql import functions as F

"url","date_download","digest","length","nlines","source_domain","title","raw_content",
group_df = deduplicated_df.groupBy("digest").agg(
    F.first("url").alias("url"),
    F.first("date_download").alias("date_download"),
    F.first("source_domain").alias("source_domain"),
    F.first("length").alias("original_length"),
    F.first("nlines").alias("original_nlines"),
    F.first("title").alias("title"),
    F.concat_ws("\n", F.collect_list("raw_line").alias("raw_content")).alias("raw_content"),
    F.count("raw_line_id").alias("nlines"),
    F.collect_list("raw_line_id").alias("line_ids"),
)
group_df=group_df.withColumn("length", F.length(group_df["raw_content"]))
if(mode_para["isTest"]):
    print("=== TestMode Log:")
    group_df.cache()
    s=time.time()
    group_df.select("url","original_length","original_nlines","raw_content","length","nlines").show(5)
    e=time.time()
    print(f"time consume:{e-s}s")


                                                                                    

### 4.7 计算留存比例



if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    origin_chars = spark_df.agg(spark_sum("length")).collect()[0][0]
    remain_chars = group_df.agg(spark_sum("length")).collect()[0][0]
    e = time.time()
    print(f"origin chars:{origin_chars/1000/1000}M,remain_chars:{remain_chars/1000/1000}M \n \
            keep chars:{round(remain_chars/origin_chars*100,3)} % time consume:{e-s}")
else:
    print("=== DevMode Log:")
    s = time.time()
    origin_chars = spark_df.agg(spark_sum("length")).collect()[0][0]
    remain_chars = group_df.agg(spark_sum("length")).collect()[0][0]
    e = time.time()
    print(f"origin chars:{origin_chars/1000/1000}M,remain_chars:{remain_chars/1000/1000}M \n \
            keep chars:{round(remain_chars/origin_chars*100,3)} % time consume:{e-s}s")


## 5. 语言识别导入



from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType

import fasttext  # type: ignore

@cached(cache={})
def getFastTextModel():
    model_path = "models/fasttext/lid.bin"
    fasttext_model = fasttext.load_model(model_path)
    return fasttext_model
def predict(model, text: str, k: int = 1):
    labels, scores = model.predict(text, k=k)
    labels = [l.replace("__label__", "") for l in labels]
    return labels, scores

@udf(returnType=StringType())
def predictLang(text):
    if not text:
        return None
    labels, scores = predict(getFastTextModel(), text.replace("\n", ""), k=1)
    scores.round(2, out=scores)
    lang = labels[0]
    score = scores[0]
    if score < 0.5:
        return None
    return lang
@udf(returnType=FloatType())
def predictScore(text):
    if not text:
        return None
    labels, scores = predict(getFastTextModel(), text.replace("\n", ""), k=1)
    scores.round(2, out=scores)
    lang = labels[0]
    score = scores[0]
    if score < 0.5:
        return None
    return float(score)
lang_df = group_df.withColumn("lang", predictLang("raw_content"))
lang_df = lang_df.withColumn("score", predictScore("raw_content"))

if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    lang_df.select("url","raw_content","lang","score").show(5)
    e = time.time()
    print(f"time consume:{e-s}s")

## 6. MultiSentencePiece 分词



from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple
import sentencepiece  # type: ignore
lm_dir: Path = Path("../cc_net/data/lm_sp")

def get_lm_languages() -> Sequence[str]:
    languages = [m.name.split(".")[0] for m in lm_dir.glob("*.arpa.bin")]
    return languages

@cached(cache={})
def getLMModel(lang):
    models={l: lm_dir / f"{l}.sp.model" for l in get_lm_languages()}
    lms=get_lm_languages()
    if(lms is None or lang not in lms):
        return None
    sp = sentencepiece.SentencePieceProcessor()
    sp.load(str(models[lang]))
    return sp

@udf(returnType=StringType())
def doSentencePiece(text,lang):
    if text is None or lang is None:
        return None
    text = text_normalizer.normalize(text)
    sp = getLMModel(lang)
    if sp is None:
        return None
    tokenized = sp.encode_as_pieces(text)
    return " ".join(tokenized)




lm_df = lang_df.withColumn("tokenized", doSentencePiece("raw_content","lang"))
if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    lm_df.select("url","raw_content","lang","score","tokenized").show(5)
    e = time.time()
    print(f"time consume:{e-s}s")


## 7. 困惑度



lm_dir: Path = Path("../cc_net/data/lm_sp")
import kenlm  # type: ignore

@cached(cache={})
def getDocLMModel(lang):
    models={l: lm_dir / f"{l}.arpa.bin" for l in get_lm_languages()}
    lms=get_lm_languages()
    if(lms is None or lang not in lms):
        return None
    lm_config = kenlm.Config()
    lm_config.load_method = 2
    lm = kenlm.Model(str(models[lang]), lm_config)
    return lm
def pp(log_score, length):
    return 10.0 ** (-log_score / length)
@udf(returnType=FloatType())
def doDocLM(text,lang):
    if text is None or lang is None:
        return None
    model = getDocLMModel(lang)
    if model is None:
        return None
    lines = text.split("\n")

    doc_log_score, doc_length = 0, 0
    for line in lines:
        log_score = model.score(line)
        length = len(line.split()) + 1
        doc_log_score += log_score
        doc_length += length
    return round(pp(doc_log_score, doc_length), 1)
doclm=getDocLMModel("en")




doclm_df = lm_df.withColumn("perplexity", doDocLM("tokenized","lang"))
if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    doclm_df.select("url","raw_content","lang","score","tokenized","perplexity").show(5)
    e = time.time()
    print(f"time consume:{e-s}s")

                                                                                    

## 8. PerplexityBucket



cutoff_csv = "../cc_net/cc_net/" + "data/" + "cutoff.csv"
percentile_head: int = 30
percentile_tail: int = 60
cutoffs = pd.read_csv(cutoff_csv, index_col=0)
cutoffs = {
    l: (cutoffs[l][percentile_head], cutoffs[l][percentile_tail])
    for l in cutoffs.columns
}

@udf(returnType=StringType())
def doPPBucket(perplexity,lang):
    if (perplexity is None):
        perplexity = -1
    if lang not in cutoffs or perplexity < 0:
        return "all"
    pp_head, pp_tail = cutoffs[lang]
    if perplexity < pp_head:
        return "head"
    if perplexity < pp_tail:
        return "middle"
    return "tail"



bucket_df = doclm_df.withColumn("bucket", doPPBucket("perplexity","lang"))
if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    bucket_df.select("url","raw_content","lang","score","tokenized","perplexity","bucket").show(50)
    e = time.time()
    print(f"time consume:{e-s}s")


                                                                                    

## 9. dropKeys



drop_df = bucket_df.drop("tokenized")
if mode_para["isTest"]:
    print("=== TestMode Log:")
    s = time.time()
    print(drop_df.summary())
    e = time.time()
    print(f"time consume:{e-s}s")


## 10. split by lang



# if mode_para["isTest"]:
#     print("=== TestMode Log:")
#     s = time.time()
#     selected_df = drop_df.filter((drop_df.lang == "en") & (drop_df.bucket == "head"))
#     selected_df.select("url","raw_content","lang","bucket").show(50)
#     e = time.time()
#     print(f"time consume:{e-s}s")


                                                                                    

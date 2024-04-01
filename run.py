# 1. 导入依赖



from ccnet_spark import open_read, parse_warc_file,compute_hashes,NaiveHashSet
from pathlib import Path
import numpy as np
import time
import pandas as pd
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StringType,IntegerType,StructType, StructField
from pyspark.sql.functions import udf, explode
from pyspark.sql.functions import sum as spark_sum

# 初始化 SparkSession
spark = SparkSession.builder.appName("CCNETSpark")  \
                    .config("spark.executor.memory", "110g") \
                    .config("spark.driver.memory", "32g") \
                    .config("spark.driver.maxResultSize", "32g") \
                    .getOrCreate()
# spark

# 2. 读取文件数据，处理成pandas DataFrame

## 2.1 获取cache文件路径



cache_data="../cache_data/2019-09/"
def getWETURL(segment: int):
    cache_file_prefix = "CC-MAIN-20190215183319-20190215205319-"
    cache_file_sufix = ".warc.wet.gz"
    segment_str = str(segment).zfill(5)  # Pad with leading zeros
    return cache_data+cache_file_prefix + segment_str + cache_file_sufix
url = getWETURL(3)
print(url)  # Output: CC-MAIN-20190215183319-20190215205319-00003.warc.wet.gz

## 2.2 处理文件，存入pandas DataFrame



def getpdf(segment,isPart:bool):
    file_path=Path(getWETURL(segment))
    file=open_read(file_path)
    s=time.time()
    pandas_df = parse_warc_file(file, 30)
    if(isPart):
        random_save_n=1000
        pandas_df = pandas_df.sample(n=random_save_n, random_state=1)
    e=time.time()
    print(f"====== parse segment:{segment} to pd_df consume:{e-s} s")
    return pandas_df

# 3. 读取 spark dataframe 文件



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


## 3.1 load spark DataFrame



segments=[i for i in range(40)]
isPart=False
s=time.time()
spark_df = getsdfs(segments,isPart=isPart)
num_docs=spark_df.count()
e=time.time()
print(f"load {len(segments)} segments,with {num_docs} docs,comsume:{e-s}s")

                                                                                    

# 4. hash计算

## 4.1 定义UDF,将doc 分割成paragraph 



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


## 4.2 udf 处理添加新字段



# 假设spark_df是您的DataFrame
# 使用UDF对raw_content字段进行处理
split_result = spark_df.withColumn("split_content", split_udf(spark_df["raw_content"]))


## 4.3 将新字段展开获取paragraph级别row



# Explode the split_content column and select the desired columns
s=time.time()
exploded_df = split_result.select("url","length","nlines","title", explode(split_result.split_content).alias("exploded_content"))

# Split the exploded_content struct into separate columns
exploded_df = exploded_df.withColumn("raw_line_id", exploded_df.exploded_content.raw_line_id)
exploded_df = exploded_df.withColumn("raw_line", exploded_df.exploded_content.raw_line)

# Drop the exploded_content column if needed
exploded_df = exploded_df.drop("exploded_content")

e=time.time()
print(f"get row id & line time consume:{e-s}s")


## 4.5 添加hash 列



import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from ccnet_spark import normalize_for_dedup
from typing import Iterable, Iterator, Sequence, Sized, Tuple, Type
HASH_TYPE: Type[np.uint64] = np.uint64
HASH_SIZE = HASH_TYPE(0).nbytes 
print(f"HASH_SIZE:{HASH_SIZE}") # 8
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
# hash_df.show()


## 4.5根据 hash 去重



deduplicated_df = hash_df.dropDuplicates(['hash_value'])


## 4.6 更新字符长度



from pyspark.sql.functions import length

# 使用 length() 函数计算字符数量，并将结果存储在新列 new_length 中
deduplicated_df = deduplicated_df.withColumn('length', length(deduplicated_df['raw_line']))


## 4.6 计算留存比例



origin_chars = spark_df.agg(spark_sum("length")).collect()[0][0]
remain_chars = deduplicated_df.agg(spark_sum("length")).collect()[0][0]
print(f"origin chars:{origin_chars/1000/1000}M,remain_chars:{remain_chars/1000/1000}M \n \
        keep chars:{round(remain_chars/origin_chars*100,3)} %")


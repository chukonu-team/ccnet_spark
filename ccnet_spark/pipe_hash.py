from typing import Type
import numpy as np
import hashlib
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType
from .text_normalizer import normalize_for_dedup
from pyspark.sql.types import (
    ArrayType,
    StringType,
    IntegerType,
    StructType,
    StructField,
)

HASH_TYPE: Type[np.uint64] = np.uint64
HASH_SIZE = HASH_TYPE(0).nbytes


# 定义一个函数，用于分割文本
@udf(ArrayType(StructType([
    StructField("raw_line_id", IntegerType(), False),
    StructField("raw_line", StringType(), False)
])))
def split_doc2para(content):
    lines = content.split("\n")
    line_ids = range(0, len(lines))  # 生成行号
    return list(zip(line_ids, lines))


@udf(returnType=StringType())
def normalize_line(line):
    if not line:
        return None
    normalized_line = normalize_for_dedup(
        line
    )  # Assuming normalize_for_dedup is defined
    return normalized_line




@udf(returnType=BinaryType())
def compute_hashes(line):
    if not line:
        return None
    normalized_line = normalize_for_dedup(
        line
    )  # Assuming normalize_for_dedup is defined
    line_hash = hashlib.sha1(bytes(normalized_line, encoding="utf-8")).digest()[
        :HASH_SIZE
    ]#8byte=8*8=64bit
    return line_hash

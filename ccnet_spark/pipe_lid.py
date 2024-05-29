from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType,IntegerType
import random
from cachetools import cached  ### model 缓存
from pyspark.sql.types import (
    StructType,
    StructField,
)
import fasttext  # type: ignore
@cached(cache={})
def getFastTextModel(fasttext_model_path):
    fasttext_model = fasttext.load_model(fasttext_model_path)
    return fasttext_model


def predict(model, text: str, k: int = 1):
    labels, scores = model.predict(text, k=k)
    labels = [label.replace("__label__", "") for label in labels]
    return labels, scores


# 定义一个函数，用于分割文本
@udf(
    StructType(
        [
            StructField("lang", StringType(), True),
            StructField("score", FloatType(), True),
        ]
    )
)
def predictLang(text,fasttext_model_path,threshold):
    if not text:
        return None, None
    labels, scores = predict(getFastTextModel(fasttext_model_path), text.replace("\n", ""), k=1)
    scores.round(2, out=scores)
    lang = labels[0]
    score = scores[0]
    if score < threshold:
        return None, None
    return lang, float(score)

# 定义一个函数，用于按照语言分区
@udf(returnType=IntegerType())
# 自定义分区函数
def custom_partitioner(lang,lang_partitions_str):
    lang_partitions = eval(lang_partitions_str)  # 将字符串转换为字典
    start, partitions = lang_partitions.get(lang, (0, 1))
    # if (partitions<=1):
    #     return 0
    partition_id = start +  random.randint(0, partitions - 1)
    return partition_id
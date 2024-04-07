from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from cachetools import cached  ### model 缓存
from pyspark.sql.types import (
    StructType,
    StructField,
)
import fasttext  # type: ignore

model_path = "/Users/zz/github/cc_net/bin/lid.bin"
@cached(cache={})
def getFastTextModel():
    fasttext_model = fasttext.load_model(model_path)
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
def predictLang(text):
    if not text:
        return None, None
    labels, scores = predict(getFastTextModel(), text.replace("\n", ""), k=1)
    scores.round(2, out=scores)
    lang = labels[0]
    score = scores[0]
    if score < 0.5:
        return None, None
    return lang, float(score)

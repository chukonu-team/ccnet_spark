from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType
from cachetools import cached  ### model 缓存
import fasttext  # type: ignore


@cached(cache={})
def getFastTextModel():
    model_path = "models/fasttext/lid.bin"
    fasttext_model = fasttext.load_model(model_path)
    return fasttext_model


def predict(model, text: str, k: int = 1):
    labels, scores = model.predict(text, k=k)
    labels = [label.replace("__label__", "") for label in labels]
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

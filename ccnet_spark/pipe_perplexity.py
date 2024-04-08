from pyspark.sql.functions import udf
from pyspark.sql.types import FloatType
from cachetools import cached  ### model 缓存
from pathlib import Path
from .pipe_tokenized import get_lm_languages 
import kenlm  # type: ignore

@cached(cache={})
def getDocLMModel(lang,lm_dir):
    lm_dir=Path(lm_dir)
    models={lang: lm_dir / f"{lang}.arpa.bin" for lang in get_lm_languages(lm_dir)}
    lms=get_lm_languages(lm_dir)
    if(lms is None or lang not in lms):
        return None
    lm_config = kenlm.Config()
    lm_config.load_method = 2
    lm = kenlm.Model(str(models[lang]), lm_config)
    return lm
def pp(log_score, length):
    return 10.0 ** (-log_score / length)
@udf(returnType=FloatType())
def doDocLM(text,lang,lm_dir):
    if text is None or lang is None:
        return None
    model = getDocLMModel(lang,lm_dir)
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
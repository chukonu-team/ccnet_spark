from typing import  Sequence
from .text_normalizer import normalize
import sentencepiece  # type: ignore
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cachetools import cached  ### model 缓存
from pathlib import Path

def get_lm_languages(lm_dir:Path) -> Sequence[str]:
    languages = [m.name.split(".")[0] for m in lm_dir.glob("*.arpa.bin")]
    return languages

@cached(cache={})
def getLMModel(lang,lm_dir):
    lm_dir=Path(lm_dir)
    models={lang_path: lm_dir / f"{lang_path}.sp.model" for lang_path in get_lm_languages(lm_dir)}
    lms=get_lm_languages(lm_dir)
    if(lms is None or lang not in lms):
        return None
    sp = sentencepiece.SentencePieceProcessor()
    sp.load(str(models[lang]))
    return sp

@udf(returnType=StringType())
def doSentencePiece(text,lang,lm_dir):
    if text is None or lang is None:
        return None
    text = normalize(text)
    sp = getLMModel(lang,lm_dir)
    if sp is None:
        return None
    tokenized = sp.encode_as_pieces(text)
    return " ".join(tokenized)
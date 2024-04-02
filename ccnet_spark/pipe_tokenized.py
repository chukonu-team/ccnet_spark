from typing import  Sequence
from .text_normalizer import normalize
import sentencepiece  # type: ignore
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from cachetools import cached  ### model 缓存
from pathlib import Path
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
    text = normalize(text)
    sp = getLMModel(lang)
    if sp is None:
        return None
    tokenized = sp.encode_as_pieces(text)
    return " ".join(tokenized)
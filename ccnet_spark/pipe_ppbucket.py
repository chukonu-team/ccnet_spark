import pandas as pd
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

cutoff_csv = "../cc_net/cc_net/" + "data/" + "cutoff.csv"
percentile_head: int = 30
percentile_tail: int = 60
cutoffs = pd.read_csv(cutoff_csv, index_col=0)
cutoffs = {
    lang: (cutoffs[lang][percentile_head], cutoffs[lang][percentile_tail])
    for lang in cutoffs.columns
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
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def doPPBucket(perplexity,lang,cutoffs_str):
    cutoffs = eval(cutoffs_str)  # 将字符串转换为字典
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
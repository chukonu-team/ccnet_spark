# ccnet pyspark version impl

## 依赖

- 安装`requirements.txt`若干依赖：`pip install -r requirements.txt`
- 手动安装kenlm:

    ```shell
    git clone https://github.com/kpu/kenlm.git
    cd kenlm
    pip install .
    ```

- 文本数据：`../cache_data/`

- 模型数据：`../cc_net/data/`

## 使用

打开 `spark_pipeline.ipynb`执行,大部分参数都hardcode了，设置mode来决定是小数据测试，还是大数据测试。

## 一些问题

- `spark_pandans_udf_pipeline.ipynb`是看网上说udf太旧了，可以考虑使用pandas udf 实现，但是实际使用pandas_udf 实现，但是发现没有udf快。
- fasttext之类的语言模型无法序列化，导致不能作为参数传入udf，这里采用`cachetools`,参见[^1]
- 统计口径，wet文件对每个网页会有`content_length`，这个cc_net代码中是直接作为doc 一个字段传入的，但是这个和代码中处理的`raw_content`长度有差距，可能原因是`content_length`包括`title`等其他字段。
然后论文中说一个shard进行去重，chars保留42%,这个spark测试发现若与content_length比较，则只有`35%`,因此重新计算len(raw_content)作为`length`，最终统计留存比例是去重前后的raw_content，然后测试发现留存比较`keep chars:46.597 %`,此外还设有min_len参数，这个是过滤掉文档长度<min_len的，因此设置不同的参数，显然会导致结果巨大差异。

[^1]: [Efficient UDFs on Databricks with unpickleable objects](https://dcferreira.com/post/2022-03-spark-serialization/)

## 评估

测试参数：使用2019-09 前4个segment，min_len=300

- 4个segment,使用pyspark ccnet pipeline（从读数据到写数据）耗时：5min,内存峰值60GB，取决于参数配置`spark.executor.memory", "64g"`
- 4个segment,使用ccnet pipeline（从读数据到写数据）耗时：时间差不多，但是本地测试发现任务调度有问题，有点job似乎失败了然后一直卡住不停止，也不重试（设置了较大的超时时间）
- ccnet 保留的doc 数量更少，但是分到head的比ccnet_spark 多

### ccnet

使用prue ccnet 获得最后的doc 数如下

- segment 1:
    all:37470
    zh_head:292
    zh_middle:390
    zh_tail:1410
    en_tail:11743
    en_head:1360
    en_middle:2526
- segment 2:
    all:38284
- segment 3:
    all:38046

### ccnet_spark

使用ccnet_spark 获得最后的doc 数如下

- segment 1:
    all:41413
    zh_head:233
    zh_middle:383
    zh_tail:1502
    en_tail:12711
    en_head:1000
    en_middle:2239
- segment 2:
    all:42100
- segment 3:
    all:41456

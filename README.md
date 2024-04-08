# ccnet pyspark version impl

## 介绍

ccnet_spark 是一个python package，使用pyspark实现CommonCrawl数据清洗pipeline,从数据下载，到去重、语言识别、文章质量分桶等多环节。

## 安装 & 卸载

### 依赖

与cc_net 相似，ccnet_spark 的一些环节依赖fasttext、kenlm、sentencepiece等

- 安装`requirements.txt`若干依赖：`pip install -r requirements.txt`
- 手动安装kenlm:

    ```shell
    git clone https://github.com/kpu/kenlm.git
    cd kenlm
    pip install .
    ```

### 安装

源码安装:

```shell
    python setup.py bdist_wheel
    cd dist
    pip install --user ccnet_spark*.whl
```

## 卸载

卸载:`pip uninstall ccnet_spark`

### 数据

需要提前下载好fastext、kenlm、sentencepiece 等模型

### 支持的pipeline

- "real_len",CommonCrawl数据中length，与实际doc length有出入，这个pipeline 是替换为实际doc length
- "hash", 计算hash，用于去重比较
- "dedup_keep"，这里进行去重，重复元素在去重后保留一份
- "dedup_nokeep"，这里进行去重，重复元素在去重后不保留
- "lid",语言识别
- "sp",分词
- "lm",语言质量打分
- "pp_bucket",根据语言质量分桶
- "drop",去掉分词字段

### 使用

具体使用参见`tests`以及[dev.md](./dev.md)

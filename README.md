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

- 使用pypi安装: `pip install ccnet_spark`
- 源码安装:

    ```shell
        python setup.py bdist_wheel
        cd dist
        pip install --user ccnet_spark*.whl
    ```

## 卸载

卸载:`pip uninstall ccnet_spark`

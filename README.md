# ccnet pyspark version impl

## 依赖

- 安装`requirements.txt`若干依赖：`pip install -r requirements.txt`
- 手动安装kenlm:

    ```shell
    git clone https://github.com/kpu/kenlm.git
    cd kenlm
    pip install .
    ```

## 使用

打开 `spark_pipeline.ipynb`执行,大部分参数都hardcode了，设置mode来决定是小数据测试，还是大数据测试。

## 一些问题

- `spark_pandans_udf_pipeline.ipynb`是看网上说udf太旧了，可以考虑使用pandas udf 实现，但是实际使用发现，使用pandas_udf 实现，但是发现没有udf快。
- fasttext之类的语言模型无法序列化，导致不能作为参数传入udf，这里采用`cachetools`,参见[^1]

[^1]: [Efficient UDFs on Databricks with unpickleable objects](https://dcferreira.com/post/2022-03-spark-serialization/)

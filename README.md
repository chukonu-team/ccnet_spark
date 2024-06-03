# ccnet pyspark version impl

## 介绍

ccnet_spark 是一个python package，使用pyspark实现CommonCrawl数据清洗pipeline,从数据下载，到去重、语言识别、文章质量分桶等多环节。

## 安装 & 卸载

- 参见 Dockerfile

### 数据

需要提前下载好kenlm、sentencepiece 等模型,默认下载到`.catched_data/`:

- `make cached_data/lid.bin`
- `make dl_all_lms`

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

#### local 模式

local模式使用docker容器测试：

- `make build_base_ccnet`
- `make run_ccnet`
- `make use_ccnet`

进入容器后执行(可能需要安装一些依赖):
这里`test_pipeline`指定参数8是指全流程，里面第二个参数是输出padas parquet路径，设为""则不输出，这用于padas 后续数据分析

- 可能需要安装最新的ccnet_spark:`make install_ccnet`
- 测试数据下载预处理环节：`make test_load`
- 测试全流程：`make test_pipeline`

### pandas 数据分析

参考jupyter notebook："local_analy.ipynb"

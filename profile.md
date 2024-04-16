# profile

如何测试在集群单节点测试，使用1segment采用0.01采样率，共400左右doc，使用.rdd.count()，可以发现pipeline:['real_len', 'hash']反而比pipeline:['real_len', 'hash', 'dedup_keep']长，说明.rdd.count()本身耗时也不少。此外，hash 这里rdd.count()内存消耗极大，110G以上，其他的最多只有60g。

- pipeline:[], time consume:1.847860336303711
- pipeline:['real_len'], time consume:1.9338457584381104
- pipeline:['real_len', 'hash'], time consume:13.863111019134521
- pipeline:['real_len', 'hash'], time consume:13.343939542770386
- pipeline:['real_len', 'hash', 'dedup_keep'], time consume:6.563436985015869
- pipeline:['real_len', 'hash', 'dedup_keep'], time consume:6.483687400817871
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid'], time consume:6.867429494857788
- pipeline:['real_len', 'hash'], time consume:13.393240213394165
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:8.507328748703003
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:9.783689737319946
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket'], time consume:10.074767351150513
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:10.004109382629395

## 采用1segment 100% 采样率，40000左右文档数量

- pipeline:[], time consume:2.738440752029419
- pipeline:['real_len'], time consume:3.108684778213501
- pipeline:['real_len', 'hash', 'dedup_keep'], time consume:27.466034412384033
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid'], time consume:32.32525587081909
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:54.34147000312805
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:543.8582842350006
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket'], time consume:61.25188612937927
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:61.04007005691528

## 采用1segment 100% 采样率，40000左右文档数量 use select

- pipeline:[], time consume:1.7016396522521973
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>], time consume:2.2142956256866455
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>], time consume:21.83850598335266
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>], time consume:22.453052759170532
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>, <PipelineStep.LID: 'lid'>], time consume:25.286282300949097
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>, <PipelineStep.LID: 'lid'>, <PipelineStep.SP: 'sp'>], time consume:46.12605571746826
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>, <PipelineStep.LID: 'lid'>, <PipelineStep.SP: 'sp'>, <PipelineStep.LM: 'lm'>], time consume:50.609835386276245
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>, <PipelineStep.LID: 'lid'>, <PipelineStep.SP: 'sp'>, <PipelineStep.LM: 'lm'>, <PipelineStep.PP_BUCKET: 'pp_bucket'>], time consume:51.27101182937622
- pipeline:[<PipelineStep.REAL_LEN: 'real_len'>, <PipelineStep.HASH: 'hash'>, <PipelineStep.DEDUP_KEEP: 'dedup_keep'>, <PipelineStep.LID: 'lid'>, <PipelineStep.SP: 'sp'>, <PipelineStep.LM: 'lm'>, <PipelineStep.PP_BUCKET: 'pp_bucket'>, <PipelineStep.DROP: 'drop'>], time consume:51.69251370429993

## 采用1segment 100% 采样率，40000左右文档数量 use select 2

- pipeline:[], time consume:1.781s
- pipeline:['real_len'], time consume:2.323s
- pipeline:['real_len', 'hash'], time consume:21.088s
- pipeline:['real_len', 'hash', 'dedup_keep'], time consume:22.473s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid'], time consume:25.669s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:46.113s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:50.316s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket'], time consume:51.804s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:52.065s

## 采用10segment 100% 采样率，40000左右文档数量 use select 2,峰值90GB，cpu 大部分时间都是跑满的，count仍然有不小overhead

- pipeline:[], time consume:3.791s
- pipeline:['real_len'], time consume:6.074s
- pipeline:['real_len', 'hash'], time consume:98.426s
- pipeline:['real_len', 'hash', 'dedup_keep'], time consume:115.58s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid'], time consume:142.949s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:242.0s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:271.934s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket'], time consume:288.895s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:728.608s,first time
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:659.178s,second time
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:286.587s,third time
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:285.602s,4 time
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:1010.266s,user save to parquet

## 采用40segment 100% 采样率，400000左右文档数量

- pipeline:['real_len', 'hash'], time consume:405.169s
- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:809.69s,峰值45.7GB
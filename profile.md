# profile

如何测试在集群单节点测试，使用1segment采用0.01采样率，共400左右doc，使用.rdd.count()，可以发现pipeline:['real_len', 'hash']反而比pipeline:['real_len', 'hash', 'dedup_keep']长，说明.rdd.count()本身耗时也不少。

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

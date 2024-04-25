# test

## 2segment 1

- pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:152.683s
- Disk nvme0n1 差值: .11 GB (读取), 1.44 GB (写入)
- Disk nvme1n1 差值: 64.45 GB (读取), 0 GB (写入)

## 2segment 2

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:188.247s

Disk nvme0n1 差值: .17 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 79.86 GB (读取), 0 GB (写入)

## 2segment 3

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:204.045s

Disk nvme0n1 差值: .15 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 100.05 GB (读取), 0 GB (写入)

## 2segment 4

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:83.451s

Disk nvme0n1 差值: .04 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 4.80 GB (读取), 0 GB (写入)

## 2segment 5

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:111.203s

Disk nvme0n1 差值: .04 GB (读取), 1.45 GB (写入)

Disk nvme1n1 差值: 7.49 GB (读取), 0 GB (写入)

## 2segment5pipe 1

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:73.007s

Disk nvme0n1 差值: 0 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: .03 GB (读取), 0 GB (写入)

## 2segment5pipe 2

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:72.914s

Disk nvme0n1 差值: 0 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 0 GB (读取), 0 GB (写入)

## 2segment5pipe 3

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp'], time consume:74.285s

Disk nvme0n1 差值: 0 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 0 GB (读取), 0 GB (写入)

## 2segment6pipe 1

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:80.194s

Disk nvme0n1 差值: .01 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 5.62 GB (读取), 0 GB (写入)

## 2segment6pipe 2

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm'], time consume:153.31s

Disk nvme0n1 差值: .15 GB (读取), 1.44 GB (写入)

Disk nvme1n1 差值: 69.17 GB (读取), 0 GB (写入)

## 40segment9pip 1

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'unknown'], time consume:2257.747s
Disk nvme0n1 差值: 13.45 GB (读取), 133.07 GB (写入)
Disk nvme1n1 差值: 651.85 GB (读取), 0 GB (写入)

## 40segment9pip 2

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'unknown'], time consume:2335.75s
Disk nvme0n1 差值: 21.97 GB (读取), 138.40 GB (写入)
Disk nvme1n1 差值: 671.38 GB (读取), 0 GB (写入)

## 40segment9pip all write to parquet

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:2304.545s
Disk nvme0n1 差值: 21.60 GB (读取), 138.83 GB (写入)
Disk nvme1n1 差值: 608.61 GB (读取), 4.10 GB (写入)
2024-04-23 03:09 INFO 20:py4j.clientserver - Closing down clientserver connection

## 40segment9pip all write to parquet 2

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:2304.545s
Disk nvme0n1 差值: 21.60 GB (读取), 138.83 GB (写入)
Disk nvme1n1 差值: 608.61 GB (读取), 4.10 GB (写入)
2024-04-23 03:09 INFO 20:py4j.clientserver - Closing down clientserver connection

## 40segment9pip all write to parquet 3

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:2376.43s
Disk nvme0n1 差值: 14.09 GB (读取), 139.24 GB (写入)
Disk nvme1n1 差值: 660.43 GB (读取), 4.12 GB (写入)

## cc_net pure,40 segment

耗时11.48-15:10=3小时+
Disk nvme0n1 差值: 1.38 GB (读取), .06 GB (写入)
Disk nvme1n1 差值: 560.97 GB (读取), 56.64 GB (写入)

## cc_net spark parquet

pipeline:['real_len', 'hash', 'dedup_keep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:2398.2s
Disk nvme0n1 差值: 26.38 GB (读取), 134.13 GB (写入)
Disk nvme1n1 差值: 621.67 GB (读取), 4.11 GB (写入)

## cc_net spark parquet no keep

pipeline:['real_len', 'hash', 'dedup_nokeep', 'lid', 'sp', 'lm', 'pp_bucket', 'drop'], time consume:2113.287s
Disk nvme0n1 差值: 10.45 GB (读取), 57.25 GB (写入)
Disk nvme1n1 差值: 597.19 GB (读取), 3.56 GB (写入)

## vs

ccnet 40 seg hash + dedup 耗时:40min====>任务调度有问题
spark 40 seg hash + dedup 耗时:10min

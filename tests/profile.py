from ccnet_spark.pipe_line import Pipeline, Config
import time

pips = [
    [],
    [
        "real_len",
    ],
    [
        "real_len",
        "hash",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
        "pp_bucket",
    ],
    [
        "real_len",
        "hash",
        "dedup_keep",
        "lid",
        "sp",
        "lm",
        "pp_bucket",
        "drop",
    ],
]
times = []
for p in pips:
    config = Config(
        isSample=False,
        n_segments=1,
        sampleRate=0.01,
        cache_dir="../cached_data/",
        dump="2019-18",
        is_cached=True,
        pipeline=p,
    )
    print(config)
    pipeline = Pipeline(config)
    df = pipeline.load_data()
    s = time.time()
    pipeline.run()
    pipeline.df.count()
    e = time.time()
    times.append(e - s)
    print(f"config:{config} time consume:{e-s}")
print(f"times:{times}")

from ccnet_spark.pipeline import Pipeline, Config
import time

config = Config(
    isSample=False,
    n_segments=8,
    sampleRate=0.1,
    cache_dir="../cached_data/",
    dump="2019-18",
    pipeline=[
        "real_len",
        "hash",
        "dedup_nokeep"
    ],
)
print(config)
pipeline = Pipeline(config)
df = pipeline.load_data()
s = time.time()
pipeline.run()
pipeline.df.show()
e = time.time()
print(e - s)

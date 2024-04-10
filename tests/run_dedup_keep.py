from ccnet_spark.pipeline import Pipeline, Config
import time

config = Config(
    isSample=True,
    n_segments=1,
    sampleRate=0.01,
    cache_dir="../cached_data/",
    dump="2019-18",

)
print(config)
pipeline = Pipeline(config)
df = pipeline.load_data()
s = time.time()
pipeline.run()
pipeline.df.count()
e = time.time()
print(e - s)

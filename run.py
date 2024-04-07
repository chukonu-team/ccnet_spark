from ccnet_spark.pipeline import Pipeline,Config
import time

config=Config(isSample=False,n_segments=2,sampleRate=0.1,cache_dir="../cache_data/",dump="2019-18")

pipeline=Pipeline(config)
df=pipeline.load_data()
s=time.time()
pipeline.run()
pipeline.save_data()
pipeline.analy()
e=time.time()
print(e-s)

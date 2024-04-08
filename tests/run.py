from ccnet_spark.pipeline import Pipeline,Config
import time

config=Config(isSample=True,n_segments=4,sampleRate=0.1,cache_dir="../cache_data/",dump="2019-18")
print(config)
pipeline=Pipeline(config)
df=pipeline.load_data()
s=time.time()
pipeline.run()
pipeline.save_data()
pipeline.analy()
e=time.time()
print(e-s)

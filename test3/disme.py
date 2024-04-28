from pyspark.sql import SparkSession

# 初始化 SparkSession
spark = SparkSession.builder \
    .appName("Demo") \
    .getOrCreate()

# 创建一个包含一组数字的 RDD
data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rdd = spark.sparkContext.parallelize(data)

# 使用 map-reduce 计算平均值
sum_count = rdd.map(lambda x: (x, 1)).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# 计算平均值
average = sum_count[0] / sum_count[1]
print("Average:", average)

# 关闭 SparkSession
spark.stop()

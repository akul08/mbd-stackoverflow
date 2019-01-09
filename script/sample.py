iirk import SparkContext

from pyspark.sql import SQLContext

from pyspark.sql import SparkSession

from pyspark.sql.functions import *



sc = SparkContext()

sc.setLogLevel("ERROR")

spark = SparkSession.builder.getOrCreate()



df = spark.read.parquet("hdfs:///user/s2125048/stackoverflow/posts") 

df1 = df.sample(False, 0.06, seed=None)

df1.write.option("compression", "gzip").json("hdfs:///user/s2087537/stackoverflow/sample2")

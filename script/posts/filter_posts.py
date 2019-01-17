from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *

# spark-submit --packages com.databricks:spark-xml_2.11:0.4.1 --master yarn \
# --deploy-mode cluster hdfs:///user/s2125048/script/filter_posts.py --num-executors 32

# running containers:   68
# allocated vCPU cores: 269
# allocated memory:     139.2G
# start:    Thu Jan 17 14:29:04 +0100 2019	
# end:      Thu Jan 17 14:49:09 +0100 2019
# elapsed:  20 min 5 sec

if __name__ == "__main__":
    
    sc = SparkContext()

    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.getOrCreate()

    home_path = "hdfs:///user/s2125048/stackoverflow/data"

    # read post data
    post = sqlContext.read.format(source="com.databricks.spark.xml").options(rowTag="row").\
                      load("{path}/raw/Posts.xml".format(path=home_path))
    # read user data
    user = spark.read.json('{path}/user/'.format(path=home_path))
    user = user.select("_AccountId","country")

    # filter year >= 2012
    post = post.withColumn("year",year(to_date(col("_CreationDate")))).\
                withColumn("month",month(to_date(col("_CreationDate"))))
    post = post.filter(post.year >= 2012)

    user = user.repartition(4)
    post = post.repartition(20)

    # join post with location
    post = post.join(user,post._OwnerUserId == user._AccountId,"left")
    post.write.option("compression","snappy").\
         partitionBy("year","month","_PostTypeId").mode('overwrite').parquet("{path}/post/".format(path=home_path))
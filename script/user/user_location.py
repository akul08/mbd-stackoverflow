from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# spark-submit --packages com.databricks:spark-xml_2.11:0.4.1 --master yarn \
# --deploy-mode cluster hdfs:///user/s2125048/script/user_location.py --num-executors 4

# running containers:   7
# allocated vCPU cores: 25
# allocated memory:     14.3G
# start:                Thu Jan 17 13:01:15 +0100 2019	
# finish:               Thu Jan 17 13:23:32 +0100 2019
# elapsed:              22 min 17 sec

if __name__ == "__main__":

    sc = SparkContext()
    sc.setLogLevel("ERROR")
    sqlContext = SQLContext(sc)
    spark = SparkSession.builder.getOrCreate()

    home_path = "hdfs:///user/s2125048/stackoverflow/data"

    ## read Users.xml
    user_fname = '{path}/raw/Users.xml'.format(path=home_path)
    user = sqlContext.read.format(source="com.databricks.spark.xml").options(rowTag="row").load(user_fname)
    user = user.select("_AccountId","_Location")

    ## lowercase and remove excessive whitespaces
    def preprocess_location(data):
        data = data.split(",")
        for i,d in enumerate(data):
            data[i] = d.strip().lower()
        return data

    preprocess_location_udf = udf(preprocess_location, ArrayType(StringType()))

    user = user.withColumn('loc_element',preprocess_location_udf(col("_Location")))
    user = user.withColumn('loc_size',size(col("loc_element")))

    ## world cities lookup
    worldcities = spark.read.csv("{path}/lookup/worldcities.csv".format(path=home_path),header=True)
    country_map = dict(worldcities.select("country").distinct().rdd.map(lambda r: (r[0],1)).collect())

    ## define location to country mapping function
    def get_country(arr,index):
        if len(arr) == index:
            try:
                x = country_map[arr[index-1]]
                return arr[index-1]
            except:
                return "invalid"
        else:
            return "invalid"

    get_country_udf = udf(get_country,StringType())

    ## map user location to country
    user = user.withColumn("country", get_country_udf(col("loc_element"),col("loc_size")))

    ## save augmented user data
    user.coalesce(1).write.mode('overwrite').json("{path}/user/".format(path=home_path))

    ## calculate and save user countries distribution
    user_country = user.select("country").groupBy("country").agg(count(lit(1)).alias("num"))    
    user_country.coalesce(1).write.mode('overwrite').csv("{path}/result/country/".format(path=home_path))
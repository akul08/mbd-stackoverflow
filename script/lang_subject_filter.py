from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

sc = SparkContext()
sc.setLogLevel("Error")
spark = SparkSession.builder.getOrCreate()

def subset_func(subset):
        print(subset.count())
        # result = subset.select('_Tags').rdd.map(lambda x: x.replace('>','').replace('<',' '))
		# 				.flatMap(lambda x: x.split(' '))
		# 				.map(lambda x: (x,1)
		# 				.reduceByKey(lambda a,b: a+b))
        # 				.sortBy(lambda x: x[1], ascending=False)
        # print(result.collect())

if __name__ == "__main__":
	data_path = "file:///home/s1745646/Project/sample2"

	if len(sys.argv) > 1:
		data_path = sys.argv[1]

	sample = spark.read.json(data_path)
	df = sample.where(sample['_Tags'].isNotNull())
	df.withColumn("_Tags", lower(col("_Tags")))


	langs = spark.read.csv("file:///home/s1745646/Project/languages.csv", header=True)
	subjects = spark.read.csv("file:///home/s1745646/Project/subjects.csv", header=True)

	subset_terms = langs.crossJoin(subjects).rdd.map(lambda x : (x.Languages.lower(), x.Subjects.lower())).collect()

	for term in subset_terms:
		lang = term[0]
		subject = term[1]
		print(lang)
		print(subject)
		subset = df.filter(df['_Tags'].contains(lang)).filter(df['_Tags'].contains(subject))
		subset_func(subset)

#print(subset_terms)
#sample.printSchema()

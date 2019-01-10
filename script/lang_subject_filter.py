from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

sc = SparkContext()
sc.setLogLevel("Error")
spark = SparkSession.builder.getOrCreate()

def subset_func(subset, csv_path, lang, subject):
	print(subset.count())
	#result = subset.select('_Tags').rdd.map(lambda x: x.replace('>','').replace('<',' ')).flatMap(lambda x: x.split(' ')).map(lambda x: (x,1).reduceByKey(lambda a,b: a+b))
	#res = result.sortBy(lambda x: x[1], ascending=False)
	#print(res.collect())
	# count no. of questions for each year
	df_ques_count = subset.groupBy(subset.year).count()
	df_ques_count.repartition(1).write.csv(csv_path+subject+'/'+lang+'_'+'ques_count.csv')

	# count total views of questions for each year
	df_views_count = subset.groupBy(subset.year).agg({'_ViewCount': 'sum'})
	df_views_count.repartition(1).write.csv(csv_path+subject+'/'+lang+'_'+'views_count.csv')

	# count total answers of questions for each year
	df_answers_count = subset.groupBy(subset.year).agg({'_AnswerCount': 'sum'})
	df_answers_count.repartition(1).write.csv(csv_path+subject+'/'+lang+'_'+'answers_count.csv')

	# count total score of questions for each year
	df_score_count = subset.groupBy(subset.year).agg({'_Score': 'sum'})
	df_score_count.repartition(1).write.csv(csv_path+subject+'/'+lang+'_'+'score_count.csv')


if __name__ == "__main__":
	data_path = "file:///home/s1745646/Project/sample2"
	csv_path = 'file:///home/s2118947/'

	if len(sys.argv) > 1:
		data_path = sys.argv[1]

	sample = spark.read.json(data_path)
	df = sample.where(sample['_Tags'].isNotNull())
	df = df.withColumn("_Tags", lower(col("_Tags")))


	langs = spark.read.csv("file:///home/s1745646/Project/languages.csv", header=True)
	subjects = spark.read.csv("file:///home/s1745646/Project/subjects.csv", header=True)

	subset_terms = langs.crossJoin(subjects).rdd.map(lambda x : (x.Languages.lower(), x.Subjects.lower())).collect()

	for term in subset_terms:
		lang = "{}".format(term[0])
		subject = "{}".format(term[1])
		print(lang)
		print(subject)
		subset = df.filter(df['_Tags'].contains(lang)).filter(df['_Tags'].contains(subject))
		subset_func(subset, csv_path, lang, subject)
#print(subset_terms)
#sample.printSchema()

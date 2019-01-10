from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import subset_filter
import sys

sc = SparkContext()
sc.setLogLevel("Error")
spark = SparkSession.builder.getOrCreate()

## Set module variable
subset_filter.spark = spark

def popularity_measure(subset, csv_path, lang, subject):
    print(subset.count())
	
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

    subset_filter.subset_filter(data_path, func=popularity_measure)
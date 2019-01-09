from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()

# change this path
sample2_path = 'file:///home/s2087537/sample2'
csv_path = 'file:///home/s2118947/'

df = spark.read.json(sample2_path)

# count no. of questions for each year
df_ques_count = df.groupBy(df.year).count()
df_ques_count.repartition(1).write.csv(csv_path+'ques_count.csv')

# count total views of questions for each year
df_views_count = df.groupBy(df.year).agg({'_ViewCount': 'sum'})
df_views_count.repartition(1).write.csv(csv_path+'views_count.csv')

# count total answers of questions for each year
df_answers_count = df.groupBy(df.year).agg({'_AnswerCount': 'sum'})
df_answers_count.repartition(1).write.csv(csv_path+'answers_count.csv')

# count total score of questions for each year
df_score_count = df.groupBy(df.year).agg({'_Score': 'sum'})
df_score_count.repartition(1).write.csv(csv_path+'score_count.csv')


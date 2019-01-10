
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *



sc = SparkContext()
sc.setLogLevel("ERROR")
spark = SparkSession.builder.getOrCreate()


topics = ['<machine-learning>','<data-science>','security', 'software']
languages = ['<python>','<scala>','<r>']

df = spark.read.json("file:///home/s2087537/sample2")
df = df.where(df['_Tags'].isNotNull())  

for x in topics:
	s1 = '.*' + x + '.*'
	for y in languages:
		df2 = df.filter(df['_Tags'].rlike(str(s1)))
		s2 = '.*' + y +'.*'
		df2 = df2.filter(df['_Tags'].rlike(str(s2)))
		df2 = df2.rdd.flatMap(lambda t: t['_Tags'].split('><'))
		df2 = df2.map(lambda t: t.replace('<','').replace('>',''))
		df2 = df2.map(lambda x: (x,1))
		df2 = df2.reduceByKey(lambda a,b: a+b)
		df2 = df2.sortBy(lambda record: record[1], ascending = False)
		outputFile = "file:///home/s2087537/" + x.replace('<','').replace('>','') + "/" + y.replace('<','').replace('>','')
		sc.parallelize(df2.take(22), numSlices = 1).saveAsTextFile(str(outputFile))
	

from pyspark import SparkContext


sc = 0

# saves the top N tags that occurr with each (topic, language) pair
def topNTags(subset, output_path, language, topic):
	# compute the most frequent N tags	
	df = subset.rdd.flatMap(lambda t: t['_Tags'].split('><'))
	df = df.map(lambda t: t.replace('<','').replace('>',''))
	df = df.map(lambda x: (x,1))
	df = df.reduceByKey(lambda a,b: a+b)
	df = df.sortBy(lambda record: record[1], ascending = False)
	# print df.take(52)
	# saving the tags as a text file
	outputFile = output_path + topic.replace('<','').replace('>','') + "/" + language.replace('<','').replace('>','')
	sc.parallelize(df.take(52), numSlices = 1).saveAsTextFile(str(outputFile))

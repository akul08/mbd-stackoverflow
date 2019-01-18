from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import subset_filter
import popularity_measure
import most_X
import topNTags
import sys

sc = SparkContext()
sc.setLogLevel("Error")
spark = SparkSession.builder.getOrCreate()

## Set module variables
subset_filter.spark = spark
topNTags.sc = sc

if __name__ == "__main__":
        data_path = "file:///home/s2118947/sample3"
        csv_path = 'file:///home/s2118947/'

        if len(sys.argv) > 1:
                data_path = sys.argv[1]

        subset_filter.subset_filter(data_path, func= (lambda subset, csv_path, lang, subject: print(lang,subject)))
	# popularity measure
        # subset_filter.subset_filter(data_path, func=popularity_measure.popularity_measure)
        # subset_filter.subset_filter(data_path, func=most_X.get_most)
	
	# compute topNTags
	#subset_filter.subset_filter(data_path, func=topNTags.topNTags)

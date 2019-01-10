from pyspark.sql.functions import *

spark = 0

def subset_func(subset, arg1, arg2, arg3):
    print(subset.count())

def subset_filter(data_path, 
                    csv_path="file:///home/s1745646/Project/ouput/",
                    langs_path="file:///home/s1745646/Project/languages.csv", 
                    subjects_path="file:///home/s1745646/Project/subjects.csv", 
                    func=subset_func):

    sample = spark.read.json(data_path)
    df = sample.where(sample['_Tags'].isNotNull())
    df = df.withColumn("_Tags", lower(col("_Tags")))

    langs = spark.read.csv(langs_path, header=True)
    subjects = spark.read.csv(subjects_path, header=True)

    subset_terms = langs.crossJoin(subjects).rdd.map(lambda x : (x.Languages.lower(), x.Subjects.lower())).collect()

    for term in subset_terms:
		lang = "{}".format(term[0])
		subject = "{}".format(term[1])
		print(lang)
		print(subject)
		subset = df.filter(df['_Tags'].contains(lang)).filter(df['_Tags'].contains(subject))
		func(subset, csv_path, lang, subject)
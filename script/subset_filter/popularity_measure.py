from pyspark.sql.functions import lit

def calculate_popularity_save_csv(subset, csv_path, lang, subject, group_column_list, filename_extension):
    # printing the current arguments
    print 'write: ', csv_path, lang, subject, group_column_list, filename_extension 

    # count no. of questions for each group_column_list
    df_ques_count = subset.groupBy(*group_column_list).count()
    df_ques_count = df_ques_count.withColumn('popularity_measure', lit('ques'))

    # count total views of questions for each group_column_list
    df_views_count = subset.groupBy(*group_column_list).agg({'_ViewCount': 'sum'})
    df_views_count = df_views_count.withColumn('popularity_measure', lit('views'))

    # count total answers of questions for each group_column_list
    df_answers_count = subset.groupBy(*group_column_list).agg({'_AnswerCount': 'sum'})
    df_answers_count = df_answers_count.withColumn('popularity_measure', lit('answers'))

    # count total score of questions for each group_column_list
    df_score_count = subset.groupBy(*group_column_list).agg({'_Score': 'sum'})
    df_score_count = df_score_count.withColumn('popularity_measure', lit('score'))

    df = df_ques_count.union(df_views_count)
    df = df.union(df_answers_count)
    df = df.union(df_score_count)
    df = df.withColumn('language', lit(lang))
    df = df.withColumn('subject', lit(subject))
    output_path = csv_path + subject + '/' + lang + '_' + filename_extension
    df.repartition(1).write.mode('overwrite').csv(output_path)


def popularity_measure(subset, csv_path, lang, subject):
    print(subset.count())
    #result = subset.select('_Tags').rdd.map(lambda x: x.replace('>','').replace('<',' ')).flatMap(lambda x: x.split(' ')).map(lambda x: (x,1).reduceByKey(lambda a,b: a+b))
    #res = result.sortBy(lambda x: x[1], ascending=False)
    #print(res.collect())

    # For Year aggregation
    # Note: passing tuple to group_column_list variable
    calculate_popularity_save_csv(subset, csv_path, lang, subject, ('year', '_PostTypeId', 'country' ), 'year')

    # For year and month aggregation
    calculate_popularity_save_csv(subset, csv_path, lang, subject, ('year', 'month', '_PostTypeId', 'country'), 'year_month_with_location')


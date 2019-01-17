def calculate_popularity_save_csv(subset, csv_path, lang, subject, group_column_list, filename_extension):
    # printing the current arguments
    print 'write: ', csv_path, lang, subject, group_column_list, filename_extension 

    # count no. of questions for each group_column_list
    output_path = csv_path+subject+'/'+lang+'_'+'ques_count_'+filename_extension
    df_ques_count = subset.groupBy(*group_column_list).count()
    df_ques_count.repartition(1).write.mode('overwrite').csv(output_path)

    # count total views of questions for each group_column_list
    output_path = csv_path+subject+'/'+lang+'_'+'views_count_'+filename_extension
    df_views_count = subset.groupBy(*group_column_list).agg({'_ViewCount': 'sum'})
    df_views_count.repartition(1).write.mode('overwrite').csv(output_path)

    # count total answers of questions for each group_column_list
    output_path = csv_path+subject+'/'+lang+'_'+'answers_count_'+filename_extension
    df_answers_count = subset.groupBy(*group_column_list).agg({'_AnswerCount': 'sum'})
    df_answers_count.repartition(1).write.mode('overwrite').csv(output_path)

    # count total score of questions for each group_column_list
    output_path = csv_path+subject+'/'+lang+'_'+'score_count_'+filename_extension
    df_score_count = subset.groupBy(*group_column_list).agg({'_Score': 'sum'})
    df_score_count.repartition(1).write.mode('overwrite').csv(output_path)


def popularity_measure(subset, csv_path, lang, subject):
    print(subset.count())
    #result = subset.select('_Tags').rdd.map(lambda x: x.replace('>','').replace('<',' ')).flatMap(lambda x: x.split(' ')).map(lambda x: (x,1).reduceByKey(lambda a,b: a+b))
    #res = result.sortBy(lambda x: x[1], ascending=False)
    #print(res.collect())

    # For Year aggregation
    # Note: passing tuple to group_column_list variable
    calculate_popularity_save_csv(subset, csv_path, lang, subject, ('year', ), 'year')

    # For year and month aggregation
    calculate_popularity_save_csv(subset, csv_path, lang, subject, ('year', 'month'), 'year_month_with_location')

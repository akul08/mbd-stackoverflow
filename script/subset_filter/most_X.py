import org.apache.spark.sql.functions._

def pretty_print(row):
    print("Title: {}".format(row['title']))
    print("Year-month: {}-{}".format(row['year'],row['month']))
    print("Body: {}".format(row['_Body']))
    print("By: {}".format(row['_OwnerDisplayName']))
    print("viewCount: {}".format(row['_ViewCount']))
    print("commentCount: {}".format(row['_CommentCount']))
    print("Score: {}".format(row['_Score']))
    print("tags: {}".format(row['_Tag']))s

def get_most(subset, csv_path, lang, subject):
    print("For topic: {} and language: {} we found the: ".format(subject,lang))

    topic_most_views = subset.sort(desc("_ViewCount")).first()
    print("Most views: ")
    pretty_print(topic_most_views)

    topic_most_answers = subset.sort(desc("_AnswerCount")).first()
    print("Most answers: ")
    pretty_print(topic_most_answers)

    topic_most_comments = subset.sort(desc("_CommentCount")).first()
    print("Most comments: ")
    pretty_print(topic_most_comments)

    topic_highest_score = subset.sort(desc("_Score")).first()
    print("Highest score: ")
    pretty_print(topic_highest_score)
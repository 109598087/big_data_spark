import time
import email.message
import smtplib


def send_email_for_notify_done(time=0):
    msg = email.message.EmailMessage()
    msg["From"] = "t109598087@ntut.org.tw"
    msg["To"] = "t109598087@ntut.org.tw"
    msg["Subject"] = "Done"

    msg.set_content("Done!")
    msg.set_content("Time:", time)

    server = smtplib.SMTP_SSL("smtp.gmail.com", 465)
    server.login("t109598087@ntut.org.tw", "Puppy802138")
    server.send_message(msg)
    server.close()


start_time = time.time()

import pandas as pd
import pyspark.pandas as ps
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


def get_df_title_headline_list(df, title_headline):
    return df[title_headline].values


def sort_words_by_most_frequent_in_descending_order(total_day_topic, df, title_headline):
    word_list = get_df_title_headline_list(df, title_headline)
    word_count_dict = dict()
    for title in word_list:
        title = str(title)
        title_split = title.split(' ')
        for word in title_split:
            word = remove_ch(word)
            if word == '':
                continue
            if word in word_count_dict:
                word_count_dict[word] += 1
            else:
                word_count_dict[word] = 0
    word_count_dict = {k: v for k, v in sorted(word_count_dict.items(), key=lambda item: item[1], reverse=True)}
    if total_day_topic != '':
        print(total_day_topic, list(word_count_dict)[:5])
    return list(word_count_dict)[:5]
    # return list(word_count_dict)[:100]
    # return list(word_count_dict)


def print_most_frequent_words_in_title_and_headline(total_day_topic, by_column):
    # remove duplicate abc
    abc_set_list = list(set(df[by_column].to_list()))
    # print
    for abc in abc_set_list:
        date_df = df[df[by_column] == abc]
        sort_words_by_most_frequent_in_descending_order(total_day_topic, date_df, 'Title')
    for abc in abc_set_list:
        date_df = df[df[by_column] == abc]
        sort_words_by_most_frequent_in_descending_order(total_day_topic, date_df, 'Headline')


def read_csv_to_ps_df(path, sqlContext):
    pd_df = pd.read_csv(path)
    column_list = list(pd_df.columns)
    schema = StructType([StructField(column, StringType(), False) for column in column_list])
    spark_df = sqlContext.createDataFrame(pd_df, schema)
    ps_df = spark_df.to_pandas_on_spark()
    ps_df.dropna()
    return ps_df


def concat_same_platform_df(platform, topic_list, sqlContext):
    feedback_df = read_csv_to_ps_df('hw2/feedback/' + platform + '_' + topic_list[0] + '.csv', sqlContext)
    for i in range(1, len(topic_list)):
        feedback_df = ps.concat(
            [feedback_df, read_csv_to_ps_df('hw2/feedback/' + platform + '_' + topic_list[i] + '.csv', sqlContext)],
            ignore_index=True)
    feedback_df['IDLink'] = feedback_df['IDLink'].astype('int64')
    return feedback_df


conf = SparkConf().setAppName('hw2').setMaster("spark://10.0.2.15:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

# read csv
df = read_csv_to_ps_df('/opt/spark/hw2/News_Final.csv', sqlContext)
# change dtype
df['SentimentTitle'] = df['SentimentTitle'].astype('float32')
df['SentimentHeadline'] = df['SentimentHeadline'].astype('float32')

print("----------------------------------------(1)------------------------------------------")
# (1) total
sort_words_by_most_frequent_in_descending_order('total', df, 'Title')
sort_words_by_most_frequent_in_descending_order('total', df, 'Headline')
# (1) per day
# PublishDate to PublishDate_date column
all_date_list = df['PublishDate'].values
df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]
print_most_frequent_words_in_title_and_headline('day', 'PublishDate_date')
# (1) per topic
print_most_frequent_words_in_title_and_headline('topic', 'Topic')

time1 = time.time() - start_time
print('1 ok:', time1)
# (2) get date, hour two column
print("----------------------------------------(2)------------------------------------------")
platform_list = ['Facebook', 'GooglePlus', 'LinkedIn']
topic_list = ['Economy', 'Microsoft', 'Obama', 'Palestine']
hours = 144 / 3
days = 2
for platform in platform_list:
    feedback_df = concat_same_platform_df(platform, topic_list, sqlContext)
    feedback_df['TS144'] = feedback_df['TS144'].astype('float32')
    feedback_df['average_popularity_by_hour'] = feedback_df['TS144'] / hours
    feedback_df['average_popularity_by_day'] = feedback_df['TS144'] / 2
    feedback_df[['IDLink', 'average_popularity_by_hour']].to_pandas().to_csv(
        'hw2/output/' + platform + 'by_hour' + '.csv')
    feedback_df[['IDLink', 'average_popularity_by_day']].to_pandas().to_csv(
        'hw2/output/' + platform + 'by_day' + '.csv')

time2 = time.time() - start_time
print('2 ok:', time2)
# (3)
print("----------------------------------------(3)------------------------------------------")
print(df.groupby('Topic').sum())
print(df.groupby('Topic').mean())

time3 = time.time() - start_time
print('3 ok:', time3)

'''
# (4)
def get_co_occurrence_matrices(word_list, title_df):
    temp_list = [0 for i in range(len(word_list))]
    # co_occurrence_matrices_df
    dict_for_df = {word_list[i]: temp_list for i in range(len(temp_list))}
    dict_for_df['index'] = word_list
    co_occurrence_matrices_df = pd.DataFrame(dict_for_df).set_index('index')
    for most_word in word_list:
        for i in range(len(word_list)):
            for title_str in title_df.to_numpy():
                title_word_list = get_title_word_list(title_str)
                # print('abc', title_word_list)
                if most_word in title_word_list and word_list[i] in title_word_list:
                    # print(most_word, most_word_of_title_list[i], title)
                    co_occurrence_matrices_df[most_word][i] += 1
    # print(co_occurrence_matrices_df)
    return co_occurrence_matrices_df


def get_title_word_list(title):
    title_word_list = title.split(' ')
    for i in range(len(title_word_list)):
        title_word_list[i] = remove_ch(title_word_list[i])
    return title_word_list


# # (4)
# print("----------------------------------------(4)------------------------------------------")
# topic_set_list = list(set(df['Topic'].to_list()))
#
# for topic in topic_set_list:
#     date_df = df[df['Topic'] == topic]
#     most_word_list = sort_words_by_most_frequent_in_descending_order('', date_df, 'Title')
#     print(get_co_occurrence_matrices(most_word_list, df['Title']))
#
# for topic in topic_set_list:
#     date_df = df[df['Topic'] == topic]
#     most_word_list = sort_words_by_most_frequent_in_descending_order('', date_df, 'Headline')
#     print(get_co_occurrence_matrices(most_word_list, df['Headline']))
#
# time4 = time.time() - start_time
# print('time', time4)
# send_email_for_notify_done(str([time1, time2, time3, time4]))
'''

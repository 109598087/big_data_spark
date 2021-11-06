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


def sort_words_by_most_frequent_in_descending_order(df, title_headline):
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
    # return word_count_dict
    return list(word_count_dict)[:5]
    # return list(word_count_dict)[:100]
    # return list(word_count_dict)


# (1)
def print_most_frequent_words_in_title_and_headline(by_column):
    # remove duplicate abc
    abc_set_list = list(set(df[by_column].to_list()))
    # print
    for abc in abc_set_list:
        date_df = df[df[by_column] == abc]
        print(abc, 'Title', sort_words_by_most_frequent_in_descending_order(date_df, 'Title'))
    for abc in abc_set_list:
        date_df = df[df[by_column] == abc]
        print(abc, 'Headline', sort_words_by_most_frequent_in_descending_order(date_df, 'Headline'))


# todo: reuse
def pd_df_to_ps_df(path, sqlContext):
    pd_df = pd.read_csv(path)
    StructField_list = list()
    StructField_list.append(StructField("IDLink", StringType(), False))
    for i in range(1, 145):
        StructField_list.append(StructField('TS' + str(i), StringType(), False))
    # StructField_list = [StructField('TS' + str(i), Floatype(), False) for i in range(1, 145)]
    schema = StructType(StructField_list)
    s_df = sqlContext.createDataFrame(pd_df, schema)
    feedback_df = s_df.to_pandas_on_spark()
    return feedback_df


# (2)
def concat_same_platform_df(platform, topic_list, sqlContext):
    feedback_df = pd_df_to_ps_df('hw2/feedback/' + platform + '_' + topic_list[0] + '.csv', sqlContext)
    for i in range(1, len(topic_list)):
        feedback_df = ps.concat(
            [feedback_df, pd_df_to_ps_df('hw2/feedback/' + platform + '_' + topic_list[i] + '.csv', sqlContext)],
            ignore_index=True)
    feedback_df['IDLink'] = feedback_df['IDLink'].astype('int64')
    return feedback_df


conf = SparkConf().setAppName('hw2').setMaster("spark://192.168.56.101:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

# read csv
pd_df = pd.read_csv('/opt/spark/hw2/News_Final.csv')
schema = StructType([
    StructField("IDLink", StringType(), False),
    StructField("Title", StringType(), False),
    StructField("Headline", StringType(), False),
    StructField("Source", StringType(), False),
    StructField("Topic", StringType(), False),
    StructField("PublishDate", StringType(), False),
    StructField("SentimentHeadline", FloatType(), False),
    StructField("SentimentTitle", FloatType(), False),
    StructField("Facebook", StringType(), False),
    StructField("GooglePlus", StringType(), False),
    StructField("LinkedIn", StringType(), False),
])
s_df = sqlContext.createDataFrame(pd_df, schema)
df = s_df.to_pandas_on_spark()
df = df.dropna()
# print(df.head())

# PublishDate to PublishDate_date column
all_date_list = df['PublishDate'].values
df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]

# (1) total
print('Title', sort_words_by_most_frequent_in_descending_order(df, 'Title'))
print('Headline', sort_words_by_most_frequent_in_descending_order(df, 'Headline'))
# (1) per day
print_most_frequent_words_in_title_and_headline('PublishDate_date')
# (1) per topic
print_most_frequent_words_in_title_and_headline('Topic')

# (2) get date, hour two column
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

# (3)
print(df.groupby('Topic').sum())
print(df.groupby('Topic').mean())


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


topic_set_list = list(set(df['Topic'].to_list()))

for topic in topic_set_list:
    date_df = df[df['Topic'] == topic]
    most_word_list = sort_words_by_most_frequent_in_descending_order(date_df, 'Title')
    print(get_co_occurrence_matrices(most_word_list, df['Title']))

for topic in topic_set_list:
    date_df = df[df['Topic'] == topic]
    most_word_list = sort_words_by_most_frequent_in_descending_order(date_df, 'Headline')
    print(get_co_occurrence_matrices(most_word_list, df['Headline']))
end_time = time.time()

print('time', end_time - start_time)
send_email_for_notify_done(str(end_time - start_time))

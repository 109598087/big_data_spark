import pyspark.pandas as ps
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from itertools import product
import pandas as pd
import time
import email.message
import smtplib

from pyspark.sql.types import *

start_time = time.time()


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', "\"", '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d', "“", "-", ' ']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


def get_word_counts_collect(df):
    df.to_csv('hw2/input/input.csv', index=False, header=False)
    words = sc.textFile("file:///opt/spark/hw2/input/input.csv").flatMap(
        lambda line: [remove_ch(word) for word in line.split(' ') if word != ''])
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    return wordCounts.collect()


def sort_word_count_collect_in_descending_order(word_count_collect):
    title_word_dict = {word_count[0]: word_count[1] for word_count in word_count_collect}
    return {k: v for k, v in sorted(title_word_dict.items(), key=lambda item: item[1], reverse=True)}


def print_word_count_dict_groupby_column_and_title_headline(title_headline, groupby_column='total'):
    if groupby_column != 'total':
        day_set_list = list(set(df[groupby_column].to_numpy()))
        for day in day_set_list:
            date_df = df[df[groupby_column] == day][title_headline]
            title_word_collect = get_word_counts_collect(date_df)
            title_word_collect = sort_word_count_collect_in_descending_order(title_word_collect)
            print(day, title_headline, list(title_word_collect)[:5])
    else:
        title_word_collect = get_word_counts_collect(df[title_headline])
        title_word_collect = sort_word_count_collect_in_descending_order(title_word_collect)
        print('total', title_headline, list(title_word_collect)[:5])


def concat_same_platform_df(platform, topic_list):
    feedback_df = ps.read_csv('File:///opt/spark/hw2/feedback/' + platform + '_' + topic_list[0] + '.csv')
    for i in range(1, len(topic_list)):
        feedback_df = ps.concat(
            [feedback_df, ps.read_csv('File:///opt/spark/hw2/feedback/' + platform + '_' + topic_list[i] + '.csv')],
            ignore_index=True)
    feedback_df['IDLink'] = feedback_df['IDLink'].astype('int64')
    return feedback_df


def get_title_word_list(title):
    title = str(title)
    title_word_list = title.split(' ')
    for i in range(len(title_word_list)):
        title_word_list[i] = remove_ch(title_word_list[i])
    return title_word_list


def get_all_topic_most_word_list(df, topic_list, title_headline):
    all_topic_most_word_list = list()  # 放所有topic 的most_word_list
    se_topic_most_word_dict = dict()  # 放各topic 的most_word_list(100*100)
    for topic in topic_list:
        se_topic_most_word_dict[topic] = get_100_words(df, topic, title_headline)
        all_topic_most_word_list += se_topic_most_word_dict[topic]
    all_topic_most_word_list = list(set(all_topic_most_word_list))
    return all_topic_most_word_list, se_topic_most_word_dict


def create_new_co_occurrence_matrices(most_word_list):
    temp_list = [0 for i in range(100)]
    co_occurrence_dict = dict()
    co_occurrence_dict['index'] = most_word_list
    for word in most_word_list:
        co_occurrence_dict[word] = temp_list
    return pd.DataFrame(co_occurrence_dict).set_index('index')


def print_co_occurrence_matrices(title_headline):
    topic_list = list(set(df['Topic'].values))

    # Title
    all_topic_most_word_list, se_topic_most_word_dict = get_all_topic_most_word_list(df, topic_list, title_headline)
    # title_split_list
    title_split_list = [get_title_word_list(title) for title in df[title_headline].to_numpy()]
    occurrence_dict = dict()
    for i in range(len(all_topic_most_word_list)):
        occurrence_dict[all_topic_most_word_list[i]] = [1 if all_topic_most_word_list[i] in title_split else 0 for
                                                        title_split in title_split_list]
    occurrence_df = pd.DataFrame(occurrence_dict)
    # print(occurrence_df)

    for topic in topic_list:
        # new co_occurrence_df
        most_word_list = se_topic_most_word_dict[topic]
        co_occurrence_df = create_new_co_occurrence_matrices(most_word_list)
        all_product_list = list(product(most_word_list, most_word_list))
        for com in all_product_list:
            occurrence_number = occurrence_df[(occurrence_df[com[0]] == 1) & (occurrence_df[com[1]] == 1)][
                com[0]].count()
            co_occurrence_df[com[0]][com[1]] = occurrence_number
        print(topic)
        print(co_occurrence_df)


def get_100_words(df, topic, title_headline):
    topic_df = df[df['Topic'] == topic][title_headline]
    word_counts_collect = get_word_counts_collect(topic_df)
    word_counts_sorted_dict = sort_word_count_collect_in_descending_order(word_counts_collect)
    return list(word_counts_sorted_dict)[:100]


def read_csv_to_ps_df(pd_df, sqlContext):
    # pd_df = pd.read_csv(path)
    column_list = list(pd_df.columns)
    schema = StructType([StructField(column, StringType(), False) for column in column_list])
    spark_df = sqlContext.createDataFrame(pd_df, schema)
    ps_df = spark_df.to_pandas_on_spark()
    ps_df.dropna()
    return ps_df


conf = SparkConf().setAppName('hw2').setMaster("spark://10.0.2.15:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

df = pd.read_csv('hw2/News_Final.csv')

# change dtype

# PublishDate to PublishDate_date column
all_date_list = df['PublishDate'].values
df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]


print("----------------------------------------(1)------------------------------------------")
print_word_count_dict_groupby_column_and_title_headline('Title', groupby_column='total')
print_word_count_dict_groupby_column_and_title_headline('Headline', groupby_column='total')

print_word_count_dict_groupby_column_and_title_headline('Title', groupby_column='PublishDate_date')
print_word_count_dict_groupby_column_and_title_headline('Headline', groupby_column='PublishDate_date')

print_word_count_dict_groupby_column_and_title_headline('Title', groupby_column='Topic')
print_word_count_dict_groupby_column_and_title_headline('Headline', groupby_column='Topic')


print("----------------------------------------(2)------------------------------------------")
platform_list = ['Facebook', 'GooglePlus', 'LinkedIn']
topic_list = ['Economy', 'Microsoft', 'Obama', 'Palestine']
hours = 144 / 3
days = 2
for platform in platform_list:
    feedback_df = concat_same_platform_df(platform, topic_list)
    feedback_df['TS144'] = feedback_df['TS144'].astype('float32')
    feedback_df['average_popularity_by_hour'] = feedback_df['TS144'] / hours
    feedback_df['average_popularity_by_day'] = feedback_df['TS144'] / days
    feedback_df[['IDLink', 'average_popularity_by_hour']].to_csv('File:///opt/spark/hw2/output/' + platform + 'by_hour')
    feedback_df[['IDLink', 'average_popularity_by_day']].to_csv(
        'File:///opt/spark/hw2/output/' + platform + 'by_day')

df1 = read_csv_to_ps_df(df, sqlContext)
df1['SentimentTitle'] = df1['SentimentTitle'].astype('float32')
df1['SentimentHeadline'] = df1['SentimentHeadline'].astype('float32')

print("----------------------------------------(3)------------------------------------------")
df1['Sentiment_score_sum'] = df1['SentimentTitle'] + df1['SentimentHeadline']
df1['Sentiment_score_mean'] = (df1['SentimentTitle'] + df1['SentimentHeadline']) / 2
print(df1.groupby('Topic')['Sentiment_score_sum'].sum())
print(df1.groupby('Topic')['Sentiment_score_mean'].mean())

print("----------------------------------------(4)------------------------------------------")
print_co_occurrence_matrices('Title')
print_co_occurrence_matrices('Headline')

end_time = time.time()
total_time = str(end_time - start_time)

print(total_time)

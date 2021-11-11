from itertools import product
import pandas as pd
import time

start_time = time.time()


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


def get_word_count_dict(df):  # todo: map reduce
    word_list = df.values
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
                word_count_dict[word] = 1
    return word_count_dict


def sort_word_count_dict_in_descending_order(word_count_dict):
    return {k: v for k, v in sorted(word_count_dict.items(), key=lambda item: item[1], reverse=True)}


def print_word_count_dict_groupby_column_and_title_headline(title_headline, groupby_column='total'):
    if groupby_column != 'total':
        day_set_list = list(set(df[groupby_column].to_list()))
        for day in day_set_list:
            date_df = df[df[groupby_column] == day][title_headline]
            word_count_dict = get_word_count_dict(date_df)
            word_count_dict = sort_word_count_dict_in_descending_order(word_count_dict)
            print(day, title_headline, list(word_count_dict)[:5])
    else:
        word_count_dict = get_word_count_dict(df[title_headline])
        word_count_dict = sort_word_count_dict_in_descending_order(word_count_dict)
        print('total', title_headline, list(word_count_dict)[:5])


# def read_csv_to_ps_df(path, sqlContext):
#     pd_df = pd.read_csv(path)
#     column_list = list(pd_df.columns)
#     schema = StructType([StructField(column, StringType(), False) for column in column_list])
#     spark_df = sqlContext.createDataFrame(pd_df, schema)
#     ps_df = spark_df.to_pandas_on_spark()
#     ps_df.dropna()
#     return ps_df


# conf = SparkConf().setAppName('hw2').setMaster("spark://10.0.2.15:7077")
# sc = SparkContext()
# sqlContext = SQLContext(sc)

# df = read_csv_to_ps_df('File:///opt/spark/hw2/News_Final.csv', sqlContext)
df = pd.read_csv('News_Final.csv', sep=',')

# change dtype
df['SentimentTitle'] = df['SentimentTitle'].astype('float32')
df['SentimentHeadline'] = df['SentimentHeadline'].astype('float32')
# PublishDate to PublishDate_date column
all_date_list = df['PublishDate'].values
df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]

print("----------------------------------------(1)------------------------------------------")
# (1) total
print_word_count_dict_groupby_column_and_title_headline('Title')
print_word_count_dict_groupby_column_and_title_headline('Headline')

# (1) day
print_word_count_dict_groupby_column_and_title_headline('Title', 'PublishDate_date')
print_word_count_dict_groupby_column_and_title_headline('Headline', 'PublishDate_date')
# (1) Topic
print_word_count_dict_groupby_column_and_title_headline('Title', 'Topic')
print_word_count_dict_groupby_column_and_title_headline('Headline', 'Topic')

print("----------------------------------------(2)------------------------------------------")


def concat_same_platform_df(platform, topic_list):
    feedback_df = pd.read_csv('../hw2/feedback/' + platform + '_' + topic_list[0] + '.csv')
    for i in range(1, len(topic_list)):
        feedback_df = pd.concat(
            [feedback_df, pd.read_csv('../hw2/feedback/' + platform + '_' + topic_list[i] + '.csv')],
            ignore_index=True)
    feedback_df['IDLink'] = feedback_df['IDLink'].astype('int64')
    return feedback_df


platform_list = ['Facebook', 'GooglePlus', 'LinkedIn']
topic_list = ['Economy', 'Microsoft', 'Obama', 'Palestine']
hours = 144 / 3
days = 2
for platform in platform_list:
    feedback_df = concat_same_platform_df(platform, topic_list)
    feedback_df['TS144'] = feedback_df['TS144'].astype('float32')
    feedback_df['average_popularity_by_hour'] = feedback_df['TS144'] / hours
    feedback_df['average_popularity_by_day'] = feedback_df['TS144'] / days
    feedback_df[['IDLink', 'average_popularity_by_hour']].to_csv('hw2/output/' + platform + 'by_hour')
    feedback_df[['IDLink', 'average_popularity_by_day']].to_csv(
        'hw2/output/' + platform + 'by_day')  # todo: to one file

# (3)
print("----------------------------------------(3)------------------------------------------")
print(df.groupby('Topic').sum())
print(df.groupby('Topic').mean())

# (4) topic
print("----------------------------------------(4)------------------------------------------")


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
        topic_df = df[df['Topic'] == topic][title_headline]
        se_topic_most_word_dict[topic] = list(sort_word_count_dict_in_descending_order(get_word_count_dict(topic_df)))[
                                         :100]
        all_topic_most_word_list += se_topic_most_word_dict[topic]
    all_topic_most_word_list = list(set(all_topic_most_word_list))
    return all_topic_most_word_list, se_topic_most_word_dict


def create_new_co_occurrence_matrices(most_word_list):
    temp_list = [0 for i in range(100)]
    co_occurrence_dict = dict()
    co_occurrence_dict['index'] = most_word_list
    for word in most_word_list:
        co_occurrence_dict[word] = temp_list
    return pd.DataFrame(co_occurrence_dict).set_index('index')  # todo pd->ps


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


print_co_occurrence_matrices('Title')
print_co_occurrence_matrices('Headline')

import email.message
import smtplib


def send_email_for_notify_done(time='0'):
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


end_time = time.time()
total_time = str(end_time - start_time)

send_email_for_notify_done(total_time)
print(total_time)

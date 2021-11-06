import pandas as pd
import time

start_time = time.time()


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0']
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


def print_most_frequent_words_in_title_and_headline(df, groupby_column):
    # remove duplicate abc
    abc_set_list = list(set(df[groupby_column].to_list()))
    # print
    for abc in abc_set_list:
        date_df = df[df[groupby_column] == abc]
        print(abc, 'Title', sort_words_by_most_frequent_in_descending_order(date_df, 'Title'))
    for abc in abc_set_list:
        date_df = df[df[groupby_column] == abc]
        print(abc, 'Headline', sort_words_by_most_frequent_in_descending_order(date_df, 'Headline'))


def concat_same_platform_df(platform, topic_list):
    feedback_df = pd.read_csv('feedback/' + platform + '_' + topic_list[0] + '.csv')
    for i in range(1, len(topic_list)):
        feedback_df = pd.concat([feedback_df, pd.read_csv('feedback/' + platform + '_' + topic_list[i] + '.csv')],
                                ignore_index=True)
    feedback_df['IDLink'] = feedback_df['IDLink'].astype('int64')
    return feedback_df


# read csv
df = pd.read_csv('News_Final.csv')
print()
column_list = list(df.columns)
print(column_list)
# df = df.dropna()
#
# # PublishDate to PublishDate_date column
# all_date_list = df['PublishDate'].values
# df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]
#
# df['Title_split_lower'] = [[remove_ch(title_s).lower() for title_s in title.split(' ')] for title in df['Title']]
# df['Headline_split_lower'] = [[remove_ch(title_s).lower() for title_s in title.split(' ')] for title in df['Headline']]
#
# # (1) total
# print('Title', sort_words_by_most_frequent_in_descending_order(df, 'Title'))
# print('Headline', sort_words_by_most_frequent_in_descending_order(df, 'Headline'))
# # (1) per day
# print_most_frequent_words_in_title_and_headline(df, 'PublishDate_date')
# # (1) per topic
# print_most_frequent_words_in_title_and_headline(df, 'Topic')
#
# # (2) get date, hour two column
# platform_list = ['Facebook', 'GooglePlus', 'LinkedIn']
# topic_list = ['Economy', 'Microsoft', 'Obama', 'Palestine']
# hours = 144 / 3
# days = 2
# for platform in platform_list:
#     feedback_df = concat_same_platform_df(platform, topic_list)
#     feedback_df['average_popularity_by_hour'] = feedback_df['TS144'] / hours
#     feedback_df['average_popularity_by_day'] = feedback_df['TS144'] / 2
#     feedback_df[['IDLink', 'average_popularity_by_hour']].to_csv('output/' + platform + 'by_hour' + '.csv')
#     feedback_df[['IDLink', 'average_popularity_by_day']].to_csv('output/' + platform + 'by_day' + '.csv')
#
# # # (3)
# print(df.groupby('Topic').sum())
# print(df.groupby('Topic').mean())
#
#
# # (4)
# topic
# most_word_list = sort_words_by_most_frequent_in_descending_order(df, title_headline)


end_time = time.time()
print('time', end_time - start_time, 's')

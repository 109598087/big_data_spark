import pandas as pd


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


# read csv
df = pd.read_csv('News_Final.csv')
df = df.dropna()
# create title_split_lower column
df['title_split_lower'] = [[remove_ch(title_s).lower() for title_s in title.split(' ')] for title in df['Title']]
df['headline_split_lower'] = [[remove_ch(title_s).lower() for title_s in title.split(' ')] for title in df['Headline']]
# print(df['title_split_lower'])
# print(df['headline_split_lower'])


# # (1) total
for title_split in df['title_split_lower']:
    print(title_split)


# print('total', sort_words_by_most_frequent_in_descending_order(df, 'Title'))
# print(sort_words_by_most_frequent_in_descending_order(df, 'Headline'))
#
# # (1) per day
# # split date_time to two column
# all_date_list = df['PublishDate'].values
# date_list = list()
# for date_time in all_date_list:
#     data_time_split = date_time.split(' ')
#     date_list.append(data_time_split[0])
# df['PublishDate_date'] = date_list
# day_groups = df.groupby('PublishDate_date').groups
# for day, index_list in day_groups.items():
#     print(day, sort_words_by_most_frequent_in_descending_order(df.loc[day_groups[day], :], 'Title'))
#
# for day, index_list in day_groups.items():
#     print(day, sort_words_by_most_frequent_in_descending_order(df.loc[day_groups[day], :], 'Headline'))
# # #
# # (1) per topic
# topic_groups = df.groupby('Topic').groups
# for topic, index_list in topic_groups.items():
#     print(topic, sort_words_by_most_frequent_in_descending_order(df.loc[topic_groups[topic], :], 'Title'))
# for topic, index_list in topic_groups.items():
#     print(topic, sort_words_by_most_frequent_in_descending_order(df.loc[topic_groups[topic], :], 'Headline'))

# # (2) get date, hour two column
# all_date_list = df['PublishDate'].values
# date_list = list()
# hour_list = list()
# for date_time in all_date_list:
#     data_time_split = date_time.split(' ')
#     date_list.append(data_time_split[0])
#     hour = data_time_split[1].split(':')[0]
#     hour_list.append(hour)
# df['PublishDate_date'] = date_list
# df['PublishDate_hour'] = hour_list
# # by day
# groupby_date_df = df.groupby('PublishDate_date')
# print(groupby_date_df['Facebook'].mean())
# print(groupby_date_df['GooglePlus'].mean())
# print(groupby_date_df['LinkedIn'].mean())
# # by hour
# groupby_hour_df = df.groupby('PublishDate_hour')
# print(groupby_hour_df['Facebook'].mean())
# print(groupby_hour_df['GooglePlus'].mean())
# print(groupby_hour_df['LinkedIn'].mean())
#
# # (3)
# print(df.groupby('Topic')['SentimentTitle'].sum())
# print(df.groupby('Topic')['SentimentHeadline'].mean())
# print(df.groupby('Topic')['SentimentTitle'].sum())
# print(df.groupby('Topic')['SentimentHeadline'].mean())

# (4)
def get_co_occurrence_matrices(word_list, title_headline_split_lower_df):
    temp_list = [0 for i in range(len(word_list))]
    # co_occurrence_matrices_df
    dict_for_df = {word_list[i]: temp_list for i in range(len(temp_list))}
    dict_for_df['index'] = word_list
    co_occurrence_matrices_df = pd.DataFrame(dict_for_df).set_index('index')
    for most_word in word_list:
        for i in range(len(word_list)):
            for title in title_headline_split_lower_df:
                if most_word in title and word_list[i] in title:
                    # print(most_word, most_word_of_title_list[i], title)
                    co_occurrence_matrices_df[most_word][i] += 1
    # print(co_occurrence_matrices_df)
    return co_occurrence_matrices_df


topic_groups = df.groupby('Topic').groups
for topic, index_list in topic_groups.items():
    word_list = sort_words_by_most_frequent_in_descending_order(df.loc[index_list, :], 'Title')
    print(get_co_occurrence_matrices(word_list, df['title_split_lower']))

for topic, index_list in topic_groups.items():
    word_list = sort_words_by_most_frequent_in_descending_order(df.loc[index_list, :], 'Headline')
    print(get_co_occurrence_matrices(word_list, df['headline_split_lower']))

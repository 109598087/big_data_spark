import pandas as pd


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


def sort_words_by_most_frequent_in_descending_order(word_list):
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
    return list(word_count_dict)[:5]
    # return list(word_count_dict)


# read csv
df = pd.read_csv('News_Final.csv')

# # (1) total
word_list1 = df['Title'].values
print(sort_words_by_most_frequent_in_descending_order(word_list1))

word_list2 = df['Headline'].values
print(sort_words_by_most_frequent_in_descending_order(word_list2))

# (1) per day
# split date_time to two column
all_date_list = df['PublishDate'].values
date_list = list()
for date_time in all_date_list:
    data_time_split = date_time.split(' ')
    date_list.append(data_time_split[0])
df['PublishDate_date'] = date_list
day_groups = df.groupby('PublishDate_date').groups

for day, index_list in day_groups.items():
    word_list = df.loc[day_groups[day], :]['Title'].values
    print(day, sort_words_by_most_frequent_in_descending_order(word_list))

for day, index_list in day_groups.items():
    word_list = df.loc[day_groups[day], :]['Headline'].values
    print(day, sort_words_by_most_frequent_in_descending_order(word_list))

# (1) per topic
topic_groups = df.groupby('Topic').groups
for topic, index_list in topic_groups.items():
    word_list = df.loc[topic_groups[topic], :]['Title'].values
    print(topic, sort_words_by_most_frequent_in_descending_order(word_list))

topic_groups = df.groupby('Topic').groups
for topic, index_list in topic_groups.items():
    word_list = df.loc[topic_groups[topic], :]['Headline'].values
    print(topic, sort_words_by_most_frequent_in_descending_order(word_list))

import pandas as pd

# read csv
df = pd.read_csv('News_Final.csv')

# split date_time to two column
all_date_list = df['PublishDate'].values
date_list = list()
time_list = list()
for date_time in all_date_list:
    data_time_split = date_time.split(' ')
    date_list.append(data_time_split[0])
    time_list.append(data_time_split[1])
# print(date_list)
# print(time_list)
df['PublishDate_date'] = date_list
df['PublishDate_time'] = time_list
# print(df.head())
# print(df.groupby('PublishDate_date'))

str1 = '\'closer'

print(str1.replace('\'', ''))


def remove_ch(word):
    remove_list = [',', '?', '\'', '$', ';', '.', '&', ':']
    for ch in remove_list:
        word = word.replace(ch, '')
    return word


all_title_list = df['Title'].values
# print(all_title_list)
word_count_dict = dict()
for title in all_title_list:
    title_split = title.split(' ')
    for word in title_split:
        word = remove_ch(word)
        if word in word_count_dict:
            word_count_dict[word] += 1
        else:
            word_count_dict[word] = 0
# print(word_count_dict)
word_count_dict = {k: v for k, v in sorted(word_count_dict.items(), key=lambda item: item[1], )}

print(word_count_dict)

import pandas as pd


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


def get_word_count_dict(df):
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


df = pd.read_csv('File:///opt/spark/hw2/News_Final.csv')
# change dtype
df['SentimentTitle'] = df['SentimentTitle'].astype('float32')
df['SentimentHeadline'] = df['SentimentHeadline'].astype('float32')
# PublishDate to PublishDate_date column
all_date_list = df['PublishDate'].values
df['PublishDate_date'] = [date_time.split(' ')[0] for date_time in all_date_list]
print(df['PublishDate_date'])

print("----------------------------------------(1)------------------------------------------")
# # (1) total
# title_headline = 'Title'
# word_count_dict = get_word_count_dict(df[title_headline])
# word_count_dict = sort_word_count_dict_in_descending_order(word_count_dict)
# print('total', title_headline, list(word_count_dict)[:5])
#
# title_headline = 'Headline'
# word_count_dict = get_word_count_dict(df['Headline'])
# word_count_dict = sort_word_count_dict_in_descending_order(word_count_dict)
# print('total', title_headline, list(word_count_dict)[:5])


# by_column = 'PublishDate_date'
# title_headline = 'Title'
# print()
# day_set_list = list(set(df[by_column].to_list()))
# for day in day_set_list:
#     print(day)
# date_df = df[df[by_column] == day][title_headline]
# word_count_dict = get_word_count_dict(date_df)
# word_count_dict = sort_word_count_dict_in_descending_order(word_count_dict)
# print(day, title_headline, list(word_count_dict)[:5])

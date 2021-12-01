from bs4 import BeautifulSoup
import pandas as pd
from os import listdir


def get_k_shingles_list(k, document):
    shingles_2_word_list = list()
    d_split = document.split(' ')
    for i in range(len(d_split) - (k - 1)):
        shingles_2_word_list.append([d_split[i + j] for j in range(k) if d_split[i + j] != ''])
    result_list = list()
    for shingles_2 in shingles_2_word_list:
        if len(shingles_2) > 1:
            result_list.append(shingles_2)
    return result_list


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d', '\n', '\x03']
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word


files = listdir('reuters21578/')
# print(files)
sgm_file_list = [file for file in files if file.endswith('.sgm')]
print(sgm_file_list)

# get_all_shingles
all_shingle_list = list()
all_body_list = list()
k = 2
for sgm_file in sgm_file_list:
    print(sgm_file, end=', ')
    file_path = 'reuters21578/' + sgm_file
    document = BeautifulSoup(open(file_path, encoding="utf-8"), 'html.parser')

    # get_2_shingles 1 file
    body_list = document.find_all('body')
    print(len(body_list))
    body_contents_list = [remove_ch(body.contents[0]) for body in body_list]
    all_body_list += body_contents_list
    k_2_shingles_list = get_k_shingles_list(k, body_contents_list[0])
    all_shingle_list += k_2_shingles_list


# print(all_shingle)
# print(len(all_shingle))
# print(all_body_list)
# print(len(all_body_list))

def shingles_to_string(k, shingles):
    str1 = ''
    for i in range(k):
        str1 += shingles[i]
        if k != len(shingles) - 1:
            str1 += ' '
    return str1


# MxN matrix
body_shingle_dict = dict()
for i in range(len(all_body_list)):
    # print(body)
    shingle_in_body_list = [1 if shingles_to_string(k, shingles) in all_body_list[i] else 0 for shingles in
                            all_shingle_list]
    body_shingle_dict[str(i)] = shingle_in_body_list

df = pd.DataFrame(body_shingle_dict)
# df['index'] = all_shingle_list
df.insert(0, 'index', all_shingle_list)
print(df)
df.to_csv('hw3_1.csv', index=False)

print(df.sum())

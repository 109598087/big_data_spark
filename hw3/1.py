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
all_shingle = list()
all_body_list = list()
for sgm_file in sgm_file_list:
    print(sgm_file)
    file_path = 'reuters21578/' + sgm_file
    document = BeautifulSoup(open(file_path, encoding="utf-8"), 'html.parser')

    # get_2_shingles 1 file
    body_list = document.find_all('body')
    all_body_list += body_list
    body_contents_list = [remove_ch(body.contents[0]) for body in body_list]
    k_2_shingles_list = get_k_shingles_list(2, body_contents_list[0])
    all_shingle += k_2_shingles_list
print(all_shingle)
print(len(all_shingle))
print(all_body_list)
print(len(all_body_list))

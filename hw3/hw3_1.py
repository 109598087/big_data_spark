from bs4 import BeautifulSoup
from pyspark import pandas as pd
import pandas as pd2
from os import listdir
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import numpy as np
from py4j.java_gateway import JavaGateway

def get_k_shingles_list(k, document):
    shingles_2_word_list = list()
    d_split = document.split(' ')
    for i in range(len(d_split) - (k - 1)):
        shingles_2_word_list.append([d_split[i + j] for j in range(k)])
    result_list = list()
    for shingles_2 in shingles_2_word_list:
        if len(shingles_2) > 1:
            result_list.append(shingles_2)
    return result_list


def remove_ch(word):
    remove_ch_list = [',', '?', '\'', '$', ';', '.', '&', ':', '!', '#', '”', '“', '£', '/', '(', ')', '"""', '\xa0',
                      '\x9d', '\n', '\x03', '\"', '+', '-', ]
    for ch in remove_ch_list:
        word = word.lower().replace(ch, '')
    return word

conf = SparkConf().setAppName('hw3').setMaster("spark://10.0.2.15:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)


files = listdir('reuters21578/')
# print(files)
sgm_file_list = [file for file in files if file.endswith('.sgm')]
# print(sgm_file_list)

# get_all_shingles
all_shingle_list = list()
all_body_list = list()

print("Please input k: ", end='')
k = int(input())
print('k = ', k)

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

print(all_shingle_list)

# all_shingle_list_np = np.array(all_shingle_list)
# print(len(all_shingle_list_np))
all_shingle_list_unique = [all_shingle_list[0]]
for shingle in all_shingle_list:
    if shingle not in all_shingle_list_unique:
        all_shingle_list_unique.append(shingle)


def shingles_to_string(k, shingles):
    str1 = ''
    for i in range(k):
        str1 += shingles[i]
        if k != len(shingles) - 1:
            str1 += ' '
    return str1


# MxN matrix
body_shingle_dict = {str(i): [1 if shingles_to_string(k, shingles) in all_body_list[i] else 0 for shingles in
                              all_shingle_list_unique] for i in range(len(all_body_list) - 1)}

df = pd2.DataFrame(body_shingle_dict)
df.insert(0, 'index', list(all_shingle_list_unique))
df.to_csv('hw3_1_' + str(k) + '.csv', index=False)

import re

from bs4 import BeautifulSoup
import pandas as pd
from os import listdir
import numpy as np


def get_document_split_list(body_document):
    body_document = body_document.replace('\n', ' ')
    d_split = re.sub("[^\w]", " ", body_document).split()
    return np.array(d_split)


def get_k_shingles_list(k, body_document):
    shingles_list = list()
    d_split = get_document_split_list(body_document)
    for i in range(len(d_split) - (k - 1)):
        shingles_list.append(np.array([d_split[i + j] for j in range(k)]))
    return np.array(shingles_list)


files = listdir('reuters21578/')
sgm_file_list = [file for file in files if file.endswith('.sgm')]

# get_all_body_element
all_body_document_list = list()
for sgm_file in sgm_file_list:
    file_path = 'reuters21578/' + sgm_file
    document = BeautifulSoup(open(file_path, encoding="utf-8"), 'html.parser')
    body_list = document.find_all('body')
    body_contents_list = [body.contents[0] for body in body_list]
    all_body_document_list += body_contents_list
all_body_document_np = np.array(all_body_document_list)
# print(all_body_document_np)
# print(len(all_body_document_np))

one_body_one_shingles_list = [get_k_shingles_list(2, body_document) for body_document in all_body_document_np]

all_shingles_list = list()
for i in range(len(all_body_document_list)):
    all_shingles_list += list(get_k_shingles_list(2, all_body_document_list[i]))
all_shingles_np = np.array(all_shingles_list)
all_shingles_unique_np = np.unique(all_shingles_np, axis=0)
print(all_shingles_unique_np)
print(len(all_shingles_np))
print(len(all_shingles_unique_np))

# MxN matrix
body_shingle_dict = {i: [1 if shingles in one_body_one_shingles_list[i] else 0 for shingles in all_shingles_np] for i in
                     range(len(one_body_one_shingles_list))}
df = pd.DataFrame(body_shingle_dict)
df.insert(0, 'index', all_shingles_np)

print(df)
print(df.sum())
df.to_csv('hw3_1_np.csv', index=False)

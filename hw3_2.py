import numpy as np
import pandas as pd
from random import shuffle
import time


def get_one_signature(df_0, shuffle_list):
    df_np = df_0.to_numpy()
    for i in range(len(shuffle_list)):
        index = np.where(shuffle_list == i + 1)[0][0]
        if df_np[index] == 1:
            return i + 1


def get_signature_list(df, shuffle_list):
    signature_list = list()
    for column in df.columns:
        if column == 'index':
            continue
        signature = get_one_signature(df[column], shuffle_list)
        signature_list.append(signature)
    return signature_list


def get_signature_matrix_df(df, all_shuffle_list):
    signature_matrix_dict = dict()
    for i in range(len(all_shuffle_list)):
        signature_matrix_dict[i] = get_signature_list(df, all_shuffle_list[i])
    return pd.DataFrame(signature_matrix_dict)


def split_vector(signature, b):
    assert len(signature) % b == 0
    r = int(len(signature) / b)
    # code splitting signature in b parts
    subvecs = []
    for i in range(0, len(signature), r):
        subvecs.append(signature[i: i + r])
    return subvecs


df = pd.read_csv('hw3/output/hw3_1_0.csv')
for i in range(1):  # todo:191
    df = df.merge(pd.read_csv('hw3/output/hw3_1_' + str(i) + '.csv'))
print(df)

# all_shuffle_list
all_shuffle_list = list()
for i in range(3):  # todo: 3 shuffle_list
    length = len(df['0'])
    a_list = [i + 1 for i in range(length)]
    shuffle(a_list)
    all_shuffle_list.append(a_list)
all_shuffle_list_np = np.array(all_shuffle_list)
# print(all_shuffle_list_np)

signature_matrix_df = get_signature_matrix_df(df, all_shuffle_list_np)
print(signature_matrix_df)
print(len(signature_matrix_df.columns))

band_list = list()
for i in range(len(signature_matrix_df.columns)):
    band_list.append(split_vector(list(signature_matrix_df[i].to_numpy()), 50))
# band_a = split_vector(list(signature_matrix_df[0].to_numpy()), 50)
#
# band_b = split_vector(list(signature_matrix_df[1].to_numpy()), 50)
#
# band_c = split_vector(list(signature_matrix_df[2].to_numpy()), 50)
from itertools import combinations

comb_list = list(combinations([i for i in range(len(band_list))], 2))
# print(comb_list)
# print(comb_list[0])

for comb in comb_list:
    for a_rows, b_rows in zip(band_list[comb[0]], band_list[comb[1]]):
        if a_rows == b_rows:
            print(f"Candidate pair: {a_rows} == {b_rows}")
            # we only need one band to match
            break

# signature_matrix_df.to_csv('sigature_matrix.csv')

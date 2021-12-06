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

print(signature_matrix_df['0'].to_numpy())

# signature_matrix_df.to_csv('sigature_matrix.csv')

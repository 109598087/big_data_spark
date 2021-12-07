import numpy as np
# from pyspark import pandas as pd  # memory?
import pandas as pd
from random import shuffle
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext


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


conf = SparkConf().setAppName('hw3').setMaster("spark://10.0.2.15:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

# df = pd.read_csv('File:///opt/spark/hw3/hw3_1.csv')
df = pd.read_csv('hw3_1.csv')
# print(df)
# for i in range(19042):
#     df[str(i)] = df[str(i)].astype('int32')

# all_shuffle_list
all_shuffle_list = list()
number = int(input('Please input hash function number: '))
for i in range(10):  # todo: 3 shuffle_list
    length = len(df['0'])
    a_list = [i + 1 for i in range(length)]
    shuffle(a_list)
    all_shuffle_list.append(a_list)
all_shuffle_list_np = np.array(all_shuffle_list)
# print(all_shuffle_list_np)

signature_matrix_df = get_signature_matrix_df(df, all_shuffle_list_np)
# print(signature_matrix_df)
signature_matrix_df.to_csv('signature_matrix.csv', index=False)

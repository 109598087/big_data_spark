from pyspark import pandas as pd
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import numpy as np

def split_vector(signature, b):
    assert len(signature) % b == 0
    r = int(len(signature) / b)
    # code splitting signature in b parts
    subvecs = []
    for i in range(0, len(signature), r):
        subvecs.append(signature[i: i + r])
    return subvecs

conf = SparkConf().setAppName('hw3').setMaster("spark://10.0.2.15:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

signature_matrix_df = pd.read_csv('File:///opt/spark/hw3/signature_matrix.csv')

band_list = list()
for i in range(len(signature_matrix_df.columns)):
    band_list.append(split_vector(list(signature_matrix_df[str(i)].to_numpy()), 9521))

from itertools import combinations

comb_list = list(combinations([i for i in range(len(band_list))], 2))
# print(comb_list)
# print(comb_list[0])
candidate_pair_list = list()
for comb in comb_list:
    for a_rows, b_rows in zip(band_list[comb[0]], band_list[comb[1]]):
        if a_rows == b_rows:
            candidate_pair_list.append(a_rows)
            #print(f"Candidate pair: {a_rows} == {b_rows}")
            # we only need one band to match
            # break
candidate_pair_list_np = np.array(candidate_pair_list)
# print(candidate_pair_list_np)
candidate_pair_list_np_unique = np.unique(candidate_pair_list_np, axis=0)
print(candidate_pair_list_np_unique)
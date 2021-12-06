import pandas as pd


def split_vector(signature, b):
    assert len(signature) % b == 0
    r = int(len(signature) / b)
    # code splitting signature in b parts
    subvecs = []
    for i in range(0, len(signature), r):
        subvecs.append(signature[i: i + r])
    return subvecs


signature_matrix_df = pd.read_csv('signature_matrix.csv')

band_list = list()
for i in range(len(signature_matrix_df.columns)):
    band_list.append(split_vector(list(signature_matrix_df[i].to_numpy()), 9521))

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

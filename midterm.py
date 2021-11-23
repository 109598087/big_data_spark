import pandas as pd

d1 = 'zika fever rash pain headache'
d2 = 'dengue fever rash muscle pain'
d3 = 'h1n1 fever muscle pain headache'

d_list = list()
d_list.append(d1)
d_list.append(d2)
d_list.append(d3)


def remove_duplicate(a_list):
    b_list = list()
    for a in a_list:
        if a not in b_list:
            b_list.append(a)
    return b_list
    # return [a for a in a_list if a not in b_list]


def get_k_shingles_list(k, document):
    shingles_2_word_list = list()
    d_split = document.split(' ')
    for i in range(len(d_split) - (k - 1)):
        shingles_2_word_list.append([d_split[i + j] for j in range(k)])
    return shingles_2_word_list


def list_to_str(a_list):
    str1 = ''
    for i in range(len(a_list)):
        str1 += a_list[i]
        if i == len(a_list) - 1:
            break
        str1 += ' '
    return str1


k = 2
shingles_2_word_list = list()
for d in d_list:
    shingles_2_word_list += get_k_shingles_list(k, d)

shingles_2_word_list = remove_duplicate(shingles_2_word_list)

characteristic_matrix_dict = dict()
for i in range(len(d_list)):
    characteristic_list = list()
    for shingles_2_word in shingles_2_word_list:
        str1 = list_to_str(shingles_2_word)
        characteristic_list.append(1 if str1 in d_list[i] else 0)
    characteristic_matrix_dict['d' + str(i + 1)] = characteristic_list
characteristic_matrix_dict['index'] = shingles_2_word_list
characteristic_matrix_df = pd.DataFrame(characteristic_matrix_dict).set_index('index')
print(characteristic_matrix_df)

# create MinHash

aasdf = 9
minhash_list = [[(2 * i + 1) % aasdf, (5 * i + 2) % aasdf] for i in range(1, aasdf + 1)]
print(minhash_list)

a_list = characteristic_matrix_df['d1']
if a_list[0] == 1:
    temp = [3, 7]
else:
    temp = [100, 100]
result_list = list()
result_list.append(temp)
for i in range(1, len(a_list)):
    if a_list[i] == 1:
        temp = [min([temp[0], minhash_list[i][0]]), min([temp[1], minhash_list[i][1]])]
    result_list.append(temp)

print(result_list)

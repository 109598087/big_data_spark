# create MinHash

aasdf = 9
minhash_list = [[(2 * i + 1) % aasdf, (5 * i + 2) % aasdf] for i in range(1, aasdf + 1)]
print(minhash_list)

# a_list = [1, 1, 1, 1, 0, 0, 0, 0, 0]
# a_list = [0, 1, 1, 0, 1, 1, 1, 0, 1]
a_list = [0, 0, 0, 1, 0, 0, 1, 1, 1]

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

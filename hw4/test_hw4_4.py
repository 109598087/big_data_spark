import pandas as pd
import numpy as np

df = pd.DataFrame({
    'A': [4, 0, 0, 5, 1, 0, 0],
    'B': [5, 5, 4, 0, 0, 0, 0],
    'C': [0, 0, 0, 2, 4, 5, 0],
})

a_np = df['A'].to_numpy()
b_np = df['C'].to_numpy()


def get_cosine_similarity(a_np, b_np):
    up = 0
    for i in range(len(a_np)):
        up += a_np[i] * b_np[i]
    return up / (pow(np.sum(np.square(a_np)), 1 / 2) * pow(np.sum(np.square(b_np)), 1 / 2))


# cosine_similarity

print(get_cosine_similarity(a_np, b_np))

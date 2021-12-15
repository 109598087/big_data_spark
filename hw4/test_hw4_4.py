import pandas as pd
import numpy as np

df = pd.DataFrame({
    'A': [4, 0, 0, 5, 1, 0, 0],
    'B': [5, 5, 4, 0, 0, 0, 0],
    'C': [0, 0, 0, 2, 4, 5, 0],
})

a_np = df['A'].to_numpy()
b_np = df['C'].to_numpy()

print(a_np * b_np)


def get_cosine_similarity(a_np, b_np):
    return np.sum(a_np * b_np) / (pow(np.sum(np.square(a_np)), 1 / 2) * pow(np.sum(np.square(b_np)), 1 / 2))



print(get_cosine_similarity(a_np, b_np))

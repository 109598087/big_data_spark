import pandas as pd

google_df = pd.read_csv('web-Google.txt', skiprows=4, sep='\t', header=None)
google_df.columns = ['FromNodeId', 'ToNodeId']

# 1
outlinks_df = google_df.groupby('FromNodeId').count().sort_values('ToNodeId', ascending=False)
outlinks_df.to_csv('output/outlinks.csv')

# 2
inlinks_df = google_df.groupby('ToNodeId').count().sort_values('FromNodeId', ascending=False)
inlinks_df.to_csv('output/inlinks.csv')

# 3
v = 0
# v = int(input())
from_v_df = google_df[google_df['FromNodeId'] == v]
print(from_v_df)
from_v_df.to_csv('output/from_v.csv', index=False)

to_v_df = google_df[google_df['ToNodeId'] == v]
print(to_v_df)
to_v_df.to_csv('output/to_v.csv', index=False)

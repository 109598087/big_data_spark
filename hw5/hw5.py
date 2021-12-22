import pandas as pd

google_df = pd.read_csv('web-Google.txt', skiprows=4, sep='\t', header=None)
google_df.columns = ['FromNodeId', 'ToNodeId']

# 1
outlinks_df = google_df.groupby('FromNodeId').count().sort_values('ToNodeId', ascending=False)
outlinks_df.to_csv('outlinks.csv')

# 2
inlinks_df = google_df.groupby('ToNodeId').count().sort_values('FromNodeId', ascending=False)
inlinks_df.to_csv('inlinks.csv')

# 3
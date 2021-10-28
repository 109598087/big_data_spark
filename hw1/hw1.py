from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pandas as pd

conf = SparkConf().setAppName('test3').setMaster("spark://192.168.56.101:7077")
sc = SparkContext()
sqlContext = SQLContext(sc)

filePath = 'household_power_consumption.csv'
df_pd = pd.read_csv(filePath, sep=';')

column_list = ['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity']
df_pd = df_pd[column_list]

# remove有?的 row
df_pd = df_pd.drop(df_pd[df_pd['Global_active_power'] == '?'].index)



# pandas DataFrame to pyspark DataFrame
schema = StructType([
    StructField("Global_active_power", StringType(), False),
    StructField("Global_reactive_power", StringType(), False),
    StructField("Voltage", StringType(), False),
    StructField("Global_intensity", StringType(), False)
    ])
df = sqlContext.createDataFrame(df_pd, schema)
print(type(df))

# change dtypes
for column in column_list:
    df = df.withColumn(column, df[column].cast('double'))


'''
# (1) Output the minimum, maximum, and count of the following columns: ‘global active power’, ‘global reactive power’, ‘voltage’, and ‘global intensity’. 
statistics_list = ['max', 'min', 'count']
max_min_count_list = [[df.agg({column: stat}).first()[0] for stat in statistics_list] for column in column_list]

max_min_count_dict = dict()
for i in range(len(column_list)):
    max_min_count_dict[column_list[i]] = max_min_count_list[i]

max_min_count_df = pd.DataFrame(max_min_count_dict, index=statistics_list)
print(max_min_count_df)

# (2) Output the mean and standard deviation of these columns.
statistics_list = ['mean', 'std']
mean_std_list = [[df.agg({column: stat}).first()[0] for stat in statistics_list] for column in column_list]

mean_std_dict = dict()
for i in range(len(column_list)):
    mean_std_dict[column_list[i]] = mean_std_list[i]

mean_std_df = pd.DataFrame(mean_std_dict, index=statistics_list)
print(mean_std_df)
'''
# (3) Perform min-max normalization on the columns to generate normalized output.
from pyspark.ml.feature import VectorAssembler, MinMaxScaler

vector_assembler = VectorAssembler(inputCols=column_list, outputCol='ss_features')
temp_train = vector_assembler.transform(df)

minmax_scaler = MinMaxScaler(inputCol='ss_features', outputCol='scaled')
train = minmax_scaler.fit(temp_train).transform(temp_train)
# train.show(2)
result_spark_df = train[['scaled']]
print(result_spark_df.show())
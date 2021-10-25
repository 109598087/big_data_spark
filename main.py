from pyspark.sql import SparkSession

spark1 = SparkSession.builder.appName("hw1").getOrCreate()
filePath = 'household_power_consumption.csv'
# df = spark.read.format("csv").option("header", "true").option("sep", ";").load(filePath)
df = spark.read.csv(filePath, inferSchema=True, header=True, sep=';')
# df.show(5)
# remove有?的 row
df = df.filter(df.Global_active_power != '?')
# change dtypes
from pyspark.sql.types import DoubleType

# print(df.dtypes)
# column_list = ['Global_active_power', 'Global_reactive_power', 'Voltage', 'Global_intensity']
column_list = ['Global_active_power']
for column in column_list:
    df = df.withColumn(column, df[column].cast('double'))
df = df[column_list]
df.show(2)
# (1) Output the minimum, maximum, and count of the following columns: ‘global active power’, ‘global reactive power’, ‘voltage’, and ‘global intensity’.
# for column in column_list:
#     df_remove_q.agg({column: 'max'}).show()
#     df_remove_q.agg({column: 'min'}).show()
#     df_remove_q.agg({column: 'count'}).show()
# # (2) Output the mean and standard deviation of these columns.
# df_remove_q.agg({column: 'mean'}).show()
# df_remove_q.agg({column: 'std'}).show()
# (3) Perform min-max normalization on the columns to generate normalized output.

from pyspark.ml.feature import VectorAssembler, StandardScaler, MinMaxScaler

vector_assembler = VectorAssembler(inputCols=column_list, outputCol='ss_features')
temp_train = vector_assembler.transform(df)
temp_train.show(2)

minmax_scaler = MinMaxScaler(inputCol='ss_features', outputCol='scaled')
train = minmax_scaler.fit(temp_train).transform(temp_train)
train.show(2)

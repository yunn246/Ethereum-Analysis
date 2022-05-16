# Scam

# import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, month, year
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType, BooleanType,TimestampType, LongType
from pyspark.sql.functions import sum, col, desc, avg, min, max, lit, count
import time

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

# read paths
transactions_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions'
scam_path = 'input/scams_distinct.csv'

# define schemas
scam_schema_df = StructType([
StructField('index', StringType(), True),
StructField('id', StringType(), True),
StructField('name', StringType(), True),
StructField('url', StringType(), True),
StructField('coin', StringType(), True),
StructField('category', StringType(), True),
StructField('subcategory', StringType(), True),
StructField('description', StringType(), True),
StructField('addresses', StringType(), True),
StructField('reporter', StringType(), True),
StructField('status', StringType(), True),
StructField('ip', StringType(), True),
StructField('nameservers', StringType(), True)
])

transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])

transaction_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)
scam_df = context.read.option("header", True).format('csv').schema(scam_schema_df).load(scam_path)


# join scam and transaction
scam_join_df = scam_df.join(transaction_df, scam_df.index ==transaction_df.to_address, how="inner")
# visualize the dataframe
# print('scam_join_df')
# scam_join_df.show()

# get the total volumn for each category in different status
lucrative_scam = scam_join_df.groupBy('category','status').agg(sum('gas'),count('gas'))

# visualize the dataframe
print('lucrative_scam')
lucrative_scam.show()
lucrative_scam.write.csv('lucrative_scam_wrong')

# check the sahpe of the dataframe
print("rows:", lucrative_scam.count(), "columns:", len(lucrative_scam.columns))

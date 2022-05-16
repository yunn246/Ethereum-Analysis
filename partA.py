# import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.context import SparkContext 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum, col, desc, udf, avg
import time

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

# read paths
transactions_path = '/data/ethereum/transactions/'

# define the schema of the transactions df
# DoubleType is for more precision
transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])

# read the transactions df
transactions_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)

# convert the timestamp to human readable time
def convert_time(x):
  time_format = "%Y.%m"
  return time.strftime(time_format,time.gmtime(x))

timestamp_udf = F.udf(convert_time, StringType())

# add the readable time column to the transaction dataframe
transactions_df = transactions_df.withColumn("gmtimestamp",timestamp_udf('block_timestamp'))
num_transactions_df = transactions_df.groupBy('gmtimestamp').count().select('gmtimestamp', F.col('count').alias('num_transaction'))
num_transactions_df.show()

# check the shape of dataframe
print("num_transactions_df - rows:",num_transactions_df.count(), "columns:", len(num_transactions_df.columns))

# aggregate on values
avg_value_df = transactions_df.groupBy('gmtimestamp').agg(F.mean('value').alias('avg_value_monthly'))
avg_value_df.show()

# check the shape of dataframe
print("avg_value_df- rows:",avg_value_df.count(), "columns:", len(avg_value_df.columns))

# save 2 files separately
num_transactions_df.write.csv('num_transactions')
avg_value_df.write.csv('avg_value')

# Wash trading

# import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.functions import to_date, month, year
from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType, LongType
from pyspark.sql.functions import sum, col, desc, avg, min, max, lit, count, first
import time

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

# read path
transactions_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions'

# define schema
transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])

# dataframe
transaction_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)


# convert time
def convert_time(x):
  time_format = "%Y.%m"
  return time.strftime(time_format,time.gmtime(x))

timestamp_udf = F.udf(convert_time, StringType())

transaction_df = transaction_df.withColumn("gmtimestamp",timestamp_udf('block_timestamp'))
print('transaction_df')
transaction_df.show()

# potential wash trading addresses
# the transaction is made from and to the same address
potential_wash = transaction_df.where(transaction_df.from_address ==  transaction_df.to_address)

# groupby the to_address and from_address (they are the same) and get the total transaction value, number of transactions and time
wt_df = potential_wash.groupBy('from_address','to_address').agg(sum('value').alias('total_trade_value'), count('value').alias('num_of_transactions'),first("gmtimestamp")).toDF('from_address', 'to_address', 'total_trade_value', 'num_of_transactions', "gmtimestamp")
# sort the dataframe to get the largest number
wash_trading = wt_df.sort(['num_of_transactions'], ascending=False)
print('wash_trading')
wash_trading.show()
print("rows:", wash_trading.count(), "columns:", len(wash_trading.columns))
# After running for the first time, get the shape of the dataframe ('rows:', 139220, 'columns:', 5)

wash_trading.write.csv('wash_same_add')

print("rows:", wash_trading.count(), "columns:", len(wash_trading.columns))

# Fork - Homestead

#import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.context import SparkContext 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.functions import sum, col, desc, udf, avg, lit
from pyspark.sql.functions import to_timestamp, when, to_date
import time

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

#path, schema, dataframe
transactions_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions'
transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])
transaction_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)

#convert time
def convert_time(x):
  time_format = "%Y-%m-%d"
  return time.strftime(time_format,time.gmtime(x))
timestamp_udf = F.udf(convert_time, StringType())
transaction_df = transaction_df.withColumn("gmtimestamp",timestamp_udf('block_timestamp'))
print('transaction_df')
transaction_df.show()

#aggregate gas and gas price
fork_df = transaction_df.groupBy('gmtimestamp').agg(avg('gas').alias('total_avg_gas'),sum('gas_price').alias('total_gas_price'), sum('value').alias('total_value'))
print('fork_df')
fork_df.show()

# find the profited most addresses
profited_most = transaction_df.groupBy('gmtimestamp','to_address').agg(sum('value').alias('address_value'))
profited_most = profited_most.sort(['address_value'], ascending=False)
print('profited_most')
profited_most.show()

# find the specific time period for Homestead
# https://ethereum.org/en/history/#homestead
# Mar-14-2016 06:49:53 PM +UTC
dates = ("2016-03-07",  "2016-03-21")
date_from, date_to = [to_date(lit(s)).cast(TimestampType()) for s in dates]

#filter to get the data for specific time period
filtered_total = fork_df.where((fork_df.gmtimestamp > date_from) & (fork_df.gmtimestamp < date_to))
print('filtered_total')
filtered_total.show()
print("rows:", filtered_total.count(), "columns:", len(filtered_total.columns))

#filter to get the data for specific time period
filtered_indiv = profited_most.where((fork_df.gmtimestamp > date_from) & (fork_df.gmtimestamp < date_to))
filtered_indiv = filtered_indiv.sort(['address_value'], ascending=False).limit(10)
print('filtered_indiv')
filtered_indiv.show()
print("rows:", filtered_indiv.count(), "columns:", len(filtered_indiv.columns))

#save the files
filtered_total.write.csv('fork_h_all')
filtered_indiv.write.csv('fork_h_profited')
print('saved')


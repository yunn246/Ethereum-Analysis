#import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.context import SparkContext 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum, col, desc, udf, avg, to_timestamp, unix_timestamp, first
import time

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)
#path
contract_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts'
transactions_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions'
blocks_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks'

#schema and dataframe
contract_schema_df = StructType([
StructField('address', StringType(), True),
StructField('is_erc20', StringType(), True),
StructField('is_erc721', StringType(), True),
StructField('block_number', IntegerType(), True),
StructField('block_timestamp', StringType(), True)])
contract_df = context.read.option("header", True).format('csv').schema(contract_schema_df).load(contract_path)

transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])
transactions_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)

blocks_schema_df = StructType([
StructField('number', StringType(), True),
StructField('hash', StringType(), True),
StructField('miner', StringType(), True),
StructField('difficulty', IntegerType(), True),
StructField('size', IntegerType(), True),
StructField('gas_limit', IntegerType(), True),
StructField('gas_used', IntegerType(), True),
StructField('timestamp', IntegerType(), True),
StructField('transaction_count', IntegerType(), True)
])
blocks_df = context.read.option("header", True).format('csv').schema(blocks_schema_df).load(blocks_path)

# convert time
def convert_time(x):
  time_format = "%Y.%m"
  return time.strftime(time_format,time.gmtime(x))
timestamp_udf = F.udf(convert_time, StringType())

#add time column
transactions_df = transactions_df.withColumn("gmtimestamp",timestamp_udf('block_timestamp'))
gas_df = transactions_df.groupBy('gmtimestamp').agg(first('to_address').alias('to_address'), sum('gas').alias('gas_used'), avg('gas_price').alias('avg_gas_price'),sum('gas_price').alias('total_gas_price'))
# print('gas_df')
# gas_df.show()
# gas_df.write.csv('gas_guz_val')

#groupby address, aggregate through gas 
gas_used_df = transactions_df.groupBy('to_address').agg(F.sum('gas').alias('gas_used'))
# print('gas_used_df')
# gas_used_df.show()

##########Have contracts become more complicated, requiring more gas, or less so?#########

#complexity of contracts
complexity_df = contract_df.join(blocks_df, contract_df.block_number==blocks_df.number, how='inner')
complexity_df = complexity_df.drop('number')
contract_complex = complexity_df.groupBy('block_timestamp','block_number').agg(avg('difficulty'),first('address').alias('address'))
# print('complexity_df')
# contract_complex.show() # no result for this one

contract_gas_used = contract_df.join(gas_used_df, contract_df.address == gas_used_df.to_address, how='inner')
contract_gas_used = contract_gas_used.drop('to_address')
# print('contract_gas_used')
# contract_gas_used.show()
contract_gas_used.write.csv('contract_gas_used')

###########How does this correlate with your results seen within Part B############

####Part B####
address_value_aggregate = transactions_df.groupBy('to_address').agg(sum('value').alias('service_value'))
joined = contract_df.join(address_value_aggregate, contract_df.address == address_value_aggregate.to_address, how='inner')
top_10 = joined.sort(['service_value'], ascending=False).limit(10)
top_10 = top_10.drop('to_address','is_erc20','is_erc721')
# print('top 10')
# top_10.show()

address_value_gas = transactions_df.groupBy('to_address').agg(sum('value'),sum('gas'))
# print('address_value_gas')
# address_value_gas.show()

contract_cor_b = address_value_gas.join(top_10, address_value_gas.to_address == top_10.address, how='inner')
contract_cor_b = contract_cor_b.sort(['service_value'], ascending=False)
# print('contract_value_gas_top10')
# contract_cor_b.show()

contract_cor_b.write.csv('contract_cor_b')

print("rows:", contract_cor_b.count(), "columns:", len(contract_cor_b.columns))


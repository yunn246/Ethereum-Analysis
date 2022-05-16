# import libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.context import SparkContext 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum, col, desc, udf, avg

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

# paths
contract_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts'
transactions_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions'

# schema of the contract, transactions
# StructField(String name, DataType dataType, boolean nullable)
# DoubleType is for more precision

contract_schema_df = StructType([
StructField('address', StringType(), True),
StructField('is_erc20', StringType(), True),
StructField('is_erc721', StringType(), True),
StructField('block_number', IntegerType(), True),
StructField('block_timestamp', StringType(), True)])

transaction_schema_df = StructType([
StructField('block_number', StringType(), True),
StructField('from_address', StringType(), True),
StructField('to_address', StringType(), True),
StructField('value', DoubleType(), True),
StructField('gas', IntegerType(), True),
StructField('gas_price', DoubleType(), True),
StructField('block_timestamp', IntegerType(), True)])

# contract df
contract_df = context.read.option("header", True).format('csv').schema(contract_schema_df).load(contract_path)
# print("contract_df")
# contract_df.show()

# transactions df
transactions_df = context.read.option("header", True).format('csv').schema(transaction_schema_df).load(transactions_path)

# JOB 1 - INITIAL AGGREGATION
address_value = transactions_df.select('to_address','value')
address_value_aggregate = address_value.groupBy('to_address').agg(sum('value'))
# print("address_value_aggregate")
# print("avg_value_df- rows:",address_value_aggregate.count(), "columns:", len(address_value_aggregate.columns))
# address_value_aggregate.show()

# JOB 2 - JOINING TRANSACTIONS/CONTRACTS AND FILTERING
joined = contract_df.join(address_value_aggregate, contract_df.address == address_value_aggregate.to_address, how='inner')
# print("joined- rows:",joined.count(), "columns:", len(joined.columns))
# joined.show()

# JOB 3 - TOP TEN
# visualizing
joined.sort(['sum(value)'], ascending=False).show(truncate=False)
# take the top 10 results
top_10 = joined.sort(['sum(value)'], ascending=False).limit(10)

#save the result to csv
top_10.write.csv('top_10')
# print("top10")

# import libraries 
import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark import SparkConf 
from pyspark.context import SparkContext 
from pyspark.sql.types import StructField, StructType, StringType, IntegerType, DoubleType
from pyspark.sql.functions import sum, col, desc, udf, avg, mean

sc = pyspark.SparkContext()
context = pyspark.SQLContext(sc)

# read path
blocks_path = 'hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/blocks'

# schema of the block dataframe
# use DoubleType for more precision
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

# read the blocks df
blocks_df = context.read.option("header", True).format('csv').schema(blocks_schema_df).load(blocks_path)


# aggregate the block size with 'miner' as key
miner_size_aggregate = blocks_df.groupBy('miner').agg(sum('size'))


# sort the value and get top 10
top_10_miners = miner_size_aggregate.sort(['sum(size)'], ascending=False).limit(10)
print("top_10_miners")

top_10_miners.write.csv('top_10_miners')
# visualize the results
top_10_miners.show()





import pyspark
import re
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as sumsql
from pyspark.sql.functions import desc


def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False

        fields[2]
        float(fields[3])


        return True
    except:
        return False

sc = pyspark.SparkContext()

sqlContext = SQLContext(sc) #enables you to run SQL and return a dataframe
transactions = sc.textFile("/data/ethereum/transactions") #reference to RDD created
scam_line = sqlContext.read.json("/user/aa363/scams.json",multiLine = True) #reference to RDD created
scam_value = scam_line.select("addresses") #using sql we select the addresses of the scams
transaction = sqlContext.read.csv("/data/ethereum/transactions",header=True) #we read the data from the csv file, header = true means first row contains column names
join = transaction.join(scam_line,col("addresses").cast("string").contains(col("from_address").cast("string"))) #join adresses from scams.json and transactions
join = join.filter(col("name").isNotNull())
mapped = join.select("category", "status", "block_timestamp" ,"value") #selects the values I want from the joined table
reduced = mapped.groupBy("block_timestamp", "category", "status").agg(sumsql("value").alias("totalVal")) #adds together all the values for a category
reduced.orderBy(desc("totalVal")).show() #shows the results and orders them in descending order from totalVal
print("Application ID", sc.applicationId)

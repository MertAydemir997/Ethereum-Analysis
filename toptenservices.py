import pyspark
import re

sc = pyspark.SparkContext()

def transClean(line):
    try:
        fields = line.split(',')
        if len(fields)!=7 or fields[2]=="":
            return False
        int(fields[3])
        return True
    except:
        return False

def contClean(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False
        return True
    except:
        return False

trans = sc.textFile("/data/ethereum/transactions")
cleanTransRows = trans.filter(transClean)
transRows = cleanTransRows.map(lambda row: (row.split(',')[2],int(row.split(',')[3])))
aggregationTrans = transRows.reduceByKey(lambda a,b: a+b)

cont = sc.textFile("/data/ethereum/contracts")
cleanContRows = cont.filter(contClean)
contRows = cleanContRows.map(lambda row:(row.split(",")[0], row.split(",")[0]))

jointValues = contRows.join(aggregationTrans)

top10=jointValues.takeOrdered(10, key = lambda x: -x[1][1])

for tuple in top10:
    print("{} {}".format(tuple[0], tuple[1][1]))

print("Application ID: ", sc.applicationId)

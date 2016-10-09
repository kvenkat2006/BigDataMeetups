from random import random
import datetime
from pyspark.sql import SparkSession
#import numpy as np

## To run this, run the following command:
# /usr/lib/spark/bin/spark-submit sparkCoreExample.py

if __name__ == "__main__":

        spark = SparkSession\
                .builder\
                .appName("RiskAggregationPOC")\
                .getOrCreate()

def makeProdAmountPairs(s):
	colVals = s.split(",")
	prodsSold = colVals[3].split("|")
	prodsSoldHash={}
	for p in prodsSold:
		ptemp = p.split(":")
		prodsSoldHash[ptemp[0]]=float(ptemp[1])
	return (colVals[2], prodsSoldHash)



def getAllKeys(a,b):
	allKeys = list(a.keys()) + list(b.keys())
	#allKeys.sort()
	keysSet = set(allKeys)
	return keysSet

def sumHashes(a,b):
	consolidatedHash={}
	keysSet = getAllKeys(a,b)
	for key in keysSet:
		sum=0.0
		if key in a.keys():
			sum = sum+a[key]
		if key in b.keys():
			sum = sum+b[key]
		consolidatedHash[key]=sum
	return consolidatedHash


pnlsRdd = spark.sparkContext.textFile("gs://bigdatacaravan.appspot.com/data/somegeneratedData.csv").filter(lambda s: "BDATE" not in s)
#pnlsRdd.repartition(14)
	
print ("XXXXXXXXXXXXXXXXXXXXXXXX  The block begins: " + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S') )

storeProductsSoldPairs = pnlsRdd.map(lambda s : makeProdAmountPairs(s)).persist()
storesSalesAmountsAggregated = storeProductsSoldPairs.reduceByKey(lambda a,b: sumHashes(a,b)).repartition(1)
storesSalesAmountsAggregated.saveAsTextFile("gs://bigdatacaravan.appspot.com/output/aggregatedTestData")

for x in storesSalesAmountsAggregated.collect():
	print("\n" + str(x) + "\n")

print ("XXXXXXXXXXXXXXXXXXXXXXXX  The  block ends: " + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S') )


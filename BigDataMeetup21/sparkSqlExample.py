from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql.functions import split, explode
## To run this, run the following command:
# /usr/lib/spark/bin/spark-submit  sparkSqlExample.py

if __name__ == "__main__":
	# $example on:init_session$
	spark = SparkSession\
		.builder\
		.appName("PythonSQL")\
		.config("spark.some.config.option", "some-value")\
		.getOrCreate()

	sc = spark.sparkContext
	sqlContext = SQLContext(sc)
	df = sqlContext.read.json("gs://bigdatacaravan.appspot.com/data/somegeneratedData.json")

	cccDf = df.select(df.SALEID, df.STORE, explode(df.PRODUCTS_SOLD).alias("prod"))
	dddDf = cccDf.select(cccDf.STORE, cccDf.prod["Amount"].alias("Amt"), cccDf.prod["PRODUCT"].alias("PRDCT"))

	eeeDf = dddDf.select(dddDf.STORE, dddDf.PRDCT, dddDf.Amt).groupby(dddDf.STORE, dddDf.PRDCT).sum().orderBy(dddDf.STORE, dddDf.PRDCT)
	eeeDf.count()
	eeeDf.show(225)

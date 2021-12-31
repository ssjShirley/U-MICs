#!/usr/bin/env python3
#Running method: spark-submit U_xmas_data_clearing.py youtube_2021-12-22.csv youtube_2021-12-23.csv youtube_2021-12-24_13_40.csv youtube_2021-12-24_11_09.csv youtube_2021-12-25_09_37\ -\ youtube_2021-12-25_09_37.csv youtube_2021-12-25_17_00.csv youtube_2021-12-26_14_00.csv [output]
#we have the datesets which using youtube_api.ipynb pull between 2021-12-22 and 2021-12-26, in this code, I was combined all stats to another table. In addition, I aslo select all description part to another table. Then we can do data analysis and prediction by such 2 tables.

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import sys,os,uuid,gzip,re,math
from collections import Counter

from pyspark.sql import SparkSession, functions, types
from datetime import datetime
from pyspark.sql.functions import lit


@functions.udf(returnType=types.IntegerType())
def getcount(count):
	return int(count)

def main(in22,in23,in24, in24_23,in25_09, in25_17, in26,output):
# read table from csv file
	y22 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in22).repartition(40)
	
	y22 = y22.withColumnRenamed("view_count","view_22").withColumnRenamed("like_count","like_22").withColumnRenamed("comment_count","comment_22")
	y23 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in23).repartition(40)
	y23 = y23.withColumnRenamed("view_count","view_23").withColumnRenamed("like_count","like_23").withColumnRenamed("comment_count","comment_23")
	y23 = y23.withColumnRenamed("view_count","view_23").withColumnRenamed("like_count","like_23").withColumnRenamed("comment_count","comment_23")
	
	y24 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in24).repartition(40)
	
	y24 = y24.withColumnRenamed("view_count","view_24").withColumnRenamed("like_count","like_24").withColumnRenamed("comment_count","comment_24")
	
	y24_23 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in24_23).repartition(40)
	
	y24_23 = y24_23.withColumnRenamed("view_count","view_24_23").withColumnRenamed("like_count","like_24_23").withColumnRenamed("comment_count","comment_24_23")
	
	y25_09 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in25_09).repartition(40)
	y25_09 = y25_09.withColumnRenamed("view_count","view_25_9").withColumnRenamed("like_count","like_25_9").withColumnRenamed("comment_count","comment_25_9")
	
	
	y25_17 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in25_17).repartition(40)
	y25_17= y25_17.withColumnRenamed("view_count","view_25_17").withColumnRenamed("like_count","like_25_17").withColumnRenamed("comment_count","comment_25_17")
	
	
	y26= spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(in26).repartition(40)
	y26= y26.withColumnRenamed("view_count","view_26").withColumnRenamed("like_count","like_26").withColumnRenamed("comment_count","comment_26")
	
	
	
	y22_stat = y22.select('video_id',"view_22","like_22","subscriber_count",'comment_22')
	y23_stat = y23.select('video_id',"view_23","like_23",'comment_23')
	y24_stat = y24.select('video_id',"view_24","like_24",'comment_24')
	y24_23_stat = y24_23.select('video_id',"view_24_23","like_24_23",'comment_24_23')
	y25_09_stat = y25_09.select('video_id',"view_25_9","like_25_9",'comment_25_9')
	y25_17_stat = y25_17.select('video_id',"view_25_17","like_25_17",'comment_25_17')
	y26_stat = y26.select('video_id',"view_26","like_26",'comment_26')
	
	xmas_stats = y22_stat.join(y23_stat,'video_id').join(y24_stat,'video_id').join(y24_23_stat,'video_id').join(y25_09_stat,'video_id').join(y25_17_stat,'video_id').join(y26_stat,'video_id')
	
	
		
	xmas_stats.coalesce(1).write.option("header", "true").csv(output+ '/stats')
	
	# print the common tags in the videp
	tags = y23.select('video_tags').rdd\
		.flatMap(lambda x: x[0].replace("[","").replace("]","").lower().split(", ")).coalesce(1)
	print(Counter(tags.collect()))
	
	# create table for statistics of the video upload today 
	time = [{'date': '12-22'},
			{'date': '12-23'},
			{'date': '12-24_noon'},
			{'date': '12-24_night'},
			{'date': '12-25_noon'},
			{'date': '12-25_night'},
			{'date': '12-26'},
	]
	
	tf = spark.createDataFrame(time)
	# get the data of 2021-12-22
	d22 = y22.filter(y22["upload_date"] == '2021-12-22').withColumn('date', lit('12-22')).withColumn('view_22', y22["view_22"].cast(types.IntegerType())).withColumn('like_22', y22["like_22"].cast(types.IntegerType())).withColumn('comment_22', y22["comment_22"].cast(types.IntegerType())).cache()
	d1 = d22.groupBy('date').agg(
		functions.count(d22['date']).alias('video_count'),
		functions.sum('view_22').alias('sum_view'),
		functions.max('view_22').alias('max_view'),
		functions.avg('view_22').alias('avg_view'),
		functions.avg('like_22').alias('avg_like'),
		functions.avg('comment_22').alias('avg_comment')	
	)
	
	# get the data of 2021-12-23
	d23 = y23.filter(y23["upload_date"] == '2021-12-23').withColumn('date', lit('12-23')).withColumn('view_23', y23["view_23"].cast(types.IntegerType())).withColumn('like_23', y23["like_23"].cast(types.IntegerType())).withColumn('comment_23', y23["comment_23"].cast(types.IntegerType())).cache()
	d2 = d23.groupBy('date').agg(
		functions.count(d23['date']).alias('video_count'),
		functions.sum('view_23').alias('sum_view'),
		functions.max('view_23').alias('max_view'),
		functions.avg('view_23').alias('avg_view'),
		functions.avg('like_23').alias('avg_like'),
		functions.avg('comment_23').alias('avg_comment')	
	)
	
	df = d1.union(d2).cache()
	
	# get the data of 2021-12-24
	d24 = y24_23.filter(y24_23["upload_date"] == '2021-12-24').withColumn('date', lit('12-24')).withColumn('view_24', y24_23["view_24_23"].cast(types.IntegerType())).withColumn('like_24', y24_23["like_24_23"].cast(types.IntegerType())).withColumn('comment_24', y24_23["comment_24_23"].cast(types.IntegerType())).cache()
	d3 = d24.groupBy('date').agg(
		functions.count(d24['date']).alias('video_count'),
		functions.sum('view_24').alias('sum_view'),
		functions.max('view_24').alias('max_view'),
		functions.avg('view_24').alias('avg_view'),
		functions.avg('like_24').alias('avg_like'),
		functions.avg('comment_24').alias('avg_comment')	
	)
	
	df = df.union(d3).cache()
	
	# get the data of 2021-12-25
	d25 = y26.filter(y26["upload_date"] == '2021-12-25').withColumn('date', lit('12-25')).withColumn('view_25', y26["view_26"].cast(types.IntegerType())).withColumn('like_25', y26["like_26"].cast(types.IntegerType())).withColumn('comment_25', y26["comment_26"].cast(types.IntegerType())).cache()
	d4 = d25.groupBy('date').agg(
		functions.count(d25['date']).alias('video_count'),
		functions.sum('view_25').alias('sum_view'),
		functions.max('view_25').alias('max_view'),
		functions.avg('view_25').alias('avg_view'),
		functions.avg('like_25').alias('avg_like'),
		functions.avg('comment_25').alias('avg_comment')	
	)
	
	df = df.union(d4).cache()
	
	# get the data of 2021-12-26
	d26 = y26.filter(y26["upload_date"] == '2021-12-26').withColumn('date', lit('12-26')).withColumn('view_26', y26["view_26"].cast(types.IntegerType())).withColumn('like_26', y26["like_26"].cast(types.IntegerType())).withColumn('comment_26', y26["comment_26"].cast(types.IntegerType())).cache()
	d5 = d26.groupBy('date').agg(
		functions.count(d26['date']).alias('video_count'),
		functions.sum('view_26').alias('sum_view'),
		functions.max('view_26').alias('max_view'),
		functions.avg('view_26').alias('avg_view'),
		functions.avg('like_26').alias('avg_like'),
		functions.avg('comment_26').alias('avg_comment')	
	)
	
	df = df.union(d5).cache()
	df.coalesce(1).write.option("header", "true").csv(output+ '/days_increasing')
	
if __name__ == '__main__':
	in22 = sys.argv[1]
	
	in23 = sys.argv[2]
	in24 = sys.argv[3]
	in24_23 = sys.argv[4]
	in25_09 = sys.argv[5]
	in25_17 = sys.argv[6]
	in26 = sys.argv[7]
	
	output = sys.argv[8]
	spark = SparkSession.builder.appName('youtube pull data clearing').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main( in22,in23,in24, in24_23,in25_09, in25_17, in26,output)
	
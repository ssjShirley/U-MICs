#!/usr/bin/env python3

#Runing method: spark-submit x_mas_feature.py youtube_2021-12-23.csv x_mas_stat.csv [output]
#This code will return a numercial table for prediction, in this new table has two part, one for description and another one for count increasing 
#- count view, like, comment increasing per day or per hald day from 2021-12-22 to 2021-12-26
#- description: covert upload date to days until 2021-12-26
#				count how many times that chrimas show in video title, tags and description
#				covert duration from string to numercial with mins

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import sys,os,uuid,gzip,re,math

from pyspark.sql import SparkSession, functions, types
from datetime import datetime
from pyspark.sql.functions import lit


YTB_stat = types.StructType([
	types.StructField('video_id', types.StringType()),
	types.StructField('view22', types.IntegerType()),
	types.StructField('like_22', types.IntegerType()),
	types.StructField('subscriber_count', types.IntegerType()),
	types.StructField('comment_22', types.IntegerType()),
	types.StructField('view23', types.IntegerType()),
	types.StructField('like_23', types.IntegerType()),
	types.StructField('comment_23', types.IntegerType()),
	types.StructField('view_24', types.IntegerType()),
	types.StructField('like_24', types.IntegerType()),
	types.StructField('comment_24', types.IntegerType()),
	types.StructField('view_24_23', types.IntegerType()),
	types.StructField('like_24_23', types.IntegerType()),
	types.StructField('comment_24_23', types.IntegerType()),
	types.StructField('view_25_9', types.IntegerType()),
	types.StructField('like_25_9', types.IntegerType()),
	types.StructField('comment_25_9', types.IntegerType()),
	types.StructField('view_25_17', types.IntegerType()),
	types.StructField('like_25_17', types.IntegerType()),
	types.StructField('comment_25_17', types.IntegerType()),
	types.StructField('view_26', types.IntegerType()),
	types.StructField('like_26', types.IntegerType()),
	types.StructField('comment_26', types.IntegerType())
	
	
])

#return upload date to days
@functions.udf(returnType=types.IntegerType())
def yeartoday(upload_data):
	if upload_data != '':
		start_date = datetime.strptime(upload_data, "%Y-%m-%d")
		end_date = datetime.strptime('12/26/2021', "%m/%d/%Y")
		return (end_date-start_date).days
		
	else:
		return 100

#return how many times christmas showing in title and description
@functions.udf(returnType=types.IntegerType())
def showfreq(str):
	if str is not None:
		str = str.lower()
		list = str.split()
		count = 0
		for i in list:
			if i =='christmas':
				count = count+1
		return count
	else:
		return 0

#return how many times christmas showing in tags
@functions.udf(returnType=types.IntegerType())
def showTagFreq(str):
	if str is not None:
		str = str.lower()
		list = str.split(',')
		count = 0
		for i in list:
			if i.find('christmas') >0:
				count = count+1
		return count
	else:
		return 0

#convert duration from string to numercial in mins	
@functions.udf(returnType=types.FloatType())
def getduration(str):
	duratuion = 0
	if str is not None :
		
		if str.find('PT')>=0:
			str = str.replace('PT', '')
		if str.find('H')>0:
			h = str.find('H')
			hr = str[:h]
			str = str[h+1:]
			if hr.isdigit():
				hour = int(hr)
			else:
				hour = 0
		else:
			hour = 0
		
		if str.find('M')>0:
			min = 0
			m = str.find('M')
			mins = str[:m]
			str = str[m+1:]
			if mins.isdigit():
				min = int(mins)
			else:
				min = 0
		else:
			min=0
		if str.find('S')>0:
			second = 0
			s = str.find('S')
			seconds = str[:s]
			str = str[s+1:]
			if seconds.isdigit():
				second = int(seconds)
			else:
				second = 0
		else:
			second = 0 
		
		duration = float(second/60*1.0)+ min +hour *60
	else:
		duration =0.0
					
	return duration

@functions.udf(returnType=types.IntegerType())
def getinscr(first, last):
	if first is not None and last is not None:
		return last-first
	
def main(input1,input2,output):
# reading data
	y23 = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(input1).repartition(40).cache()

		
	data = spark.read.option("header","true").csv(input2, schema=YTB_stat).cache()

	
	#elt clearning data for prediction	
	d1 = y23.select('video_id',
		yeartoday('upload_date').alias('upload_days'),
		showfreq('video_title').alias('title'),
		showfreq('video_description').alias('description'),
		showTagFreq('video_tags').alias('tags'),
		getduration("duration_count").alias('duration')
	).cache()
	d2 = data.select('video_id', 'subscriber_count', 'view22', 'like_22','comment_22',
		getinscr("view22",'view23').alias('view_inc_23'),
		getinscr("view23",'view_24').alias('view_inc_24_noon'),
		getinscr("view_24",'view_24_23').alias('view_inc_24_night'),
		getinscr("view_24_23",'view_25_9').alias('view_inc_25_noon'),
		getinscr("view_25_9",'view_25_17').alias('view_inc_25_night'),
		
		getinscr("like_22",'like_23').alias('like_inc_23'),
		getinscr("like_23",'like_24').alias('like_inc_24_noon'),
		getinscr("like_24",'like_24_23').alias('like_inc_24_night'),
		getinscr("like_24_23",'like_25_9').alias('like_inc_25_noon'),
		getinscr("like_25_9",'like_25_17').alias('like_inc_25_night'),
		
		getinscr("comment_22",'comment_23').alias('comment_inc_23'),
		getinscr("comment_23",'comment_24').alias('comment_inc_24_noon'),
		getinscr("comment_24",'comment_24_23').alias('comment_inc_24_night'),
		getinscr("comment_24_23",'comment_25_9').alias('comment_inc_25_noon'),
		getinscr("comment_25_9",'comment_25_17').alias('comment_inc_25_night'),
		'like_26','comment_26','view_26'
		
		
	).cache()
	
	d3 = d1.join(d2,'video_id')
	d3.coalesce(1).write.option("header", "true").csv(output +'/clearing table')
	
	#find the stat increase for total date and get the sum view, max view, average view, avg like and avg comment
	#get data of 2021-12-23
	d23 = d2.select('view_inc_23','like_inc_23','comment_inc_23').withColumn('date', lit('12-23')).cache()
	t1 = d23.groupBy('date').agg(
		functions.sum('view_inc_23').alias('sum_view'),
		functions.max('view_inc_23').alias('max_view'),
		functions.avg('view_inc_23').alias('avg_view'),
		functions.avg('like_inc_23').alias('avg_like'),
		functions.avg('comment_inc_23').alias('avg_comment')	
	)
	
	#get data of 2021-12-24
	d24_noon = d2.select('view_inc_24_noon','like_inc_24_noon','comment_inc_24_noon').withColumn('date', lit('12-24_noon')).cache()
	t2 = d24_noon.groupBy('date').agg(
		
		functions.sum('view_inc_24_noon').alias('sum_view'),
		functions.max('view_inc_24_noon').alias('max_view'),
		functions.avg('view_inc_24_noon').alias('avg_view'),
		functions.avg('like_inc_24_noon').alias('avg_like'),
		functions.avg('comment_inc_24_noon').alias('avg_comment')	
	)
	
	
	d24_night = d2.select('view_inc_24_night','like_inc_24_night','comment_inc_24_night').withColumn('date', lit('12-24_night')).cache()
	t3 = d24_night.groupBy('date').agg(
		
		functions.sum('view_inc_24_night').alias('sum_view'),
		functions.max('view_inc_24_night').alias('max_view'),
		functions.avg('view_inc_24_night').alias('avg_view'),
		functions.avg('like_inc_24_night').alias('avg_like'),
		functions.avg('comment_inc_24_night').alias('avg_comment')	
	)
	tf = t1.union(t2).cache()
	tf = tf.union(t3).cache()
	
	#get data of 2021-12-25
	d25_noon = d2.select('view_inc_25_noon','like_inc_25_noon','comment_inc_25_noon').withColumn('date', lit('12-25_noon')).cache()
	t4 = d25_noon.groupBy('date').agg(
		
		functions.sum('view_inc_25_noon').alias('sum_view'),
		functions.max('view_inc_25_noon').alias('max_view'),
		functions.avg('view_inc_25_noon').alias('avg_view'),
		functions.avg('like_inc_25_noon').alias('avg_like'),
		functions.avg('comment_inc_25_noon').alias('avg_comment')	
	)
	
	
	d25_night = d2.select('view_inc_25_night','like_inc_25_night','comment_inc_25_night').withColumn('date', lit('12-25_night')).cache()
	t5 = d25_night.groupBy('date').agg(
		
		functions.sum('view_inc_25_night').alias('sum_view'),
		functions.max('view_inc_25_night').alias('max_view'),
		functions.avg('view_inc_25_night').alias('avg_view'),
		functions.avg('like_inc_25_night').alias('avg_like'),
		functions.avg('comment_inc_25_night').alias('avg_comment')	
	)
	
	tf = tf.union(t4).union(t5).cache()
	tf.coalesce(1).write.option("header", "true").csv(output +'/total inc')	
	
	
	
	#find the stat increase for top 200 date order by 12-22 view and get the sum view, max view, average view, avg like and avg comment
	d4 = d2.orderBy('view22', ascending=False).limit(500).cache()
	
	
	#get data of 2021-12-23
	d23 = d4.select('view_inc_23','like_inc_23','comment_inc_23').withColumn('date', lit('12-23')).cache()
	t1 = d23.groupBy('date').agg(
		functions.sum('view_inc_23').alias('sum_view'),
		functions.max('view_inc_23').alias('max_view'),
		functions.avg('view_inc_23').alias('avg_view'),
		functions.avg('like_inc_23').alias('avg_like'),
		functions.avg('comment_inc_23').alias('avg_comment')	
	)
	
	#get data of 2021-12-24
	d24_noon = d4.select('view_inc_24_noon','like_inc_24_noon','comment_inc_24_noon').withColumn('date', lit('12-24_noon')).cache()
	t2 = d24_noon.groupBy('date').agg(
		
		functions.sum('view_inc_24_noon').alias('sum_view'),
		functions.max('view_inc_24_noon').alias('max_view'),
		functions.avg('view_inc_24_noon').alias('avg_view'),
		functions.avg('like_inc_24_noon').alias('avg_like'),
		functions.avg('comment_inc_24_noon').alias('avg_comment')	
	)
	
	
	d24_night = d4.select('view_inc_24_night','like_inc_24_night','comment_inc_24_night').withColumn('date', lit('12-24_night')).cache()
	t3 = d24_night.groupBy('date').agg(
		
		functions.sum('view_inc_24_night').alias('sum_view'),
		functions.max('view_inc_24_night').alias('max_view'),
		functions.avg('view_inc_24_night').alias('avg_view'),
		functions.avg('like_inc_24_night').alias('avg_like'),
		functions.avg('comment_inc_24_night').alias('avg_comment')	
	)
	tf = t1.union(t2).union(t3).cache()
	
	#get data of 2021-12-25
	d25_noon = d4.select('view_inc_25_noon','like_inc_25_noon','comment_inc_25_noon').withColumn('date', lit('12-25_noon')).cache()
	t4 = d25_noon.groupBy('date').agg(
		
		functions.sum('view_inc_25_noon').alias('sum_view'),
		functions.max('view_inc_25_noon').alias('max_view'),
		functions.avg('view_inc_25_noon').alias('avg_view'),
		functions.avg('like_inc_25_noon').alias('avg_like'),
		functions.avg('comment_inc_25_noon').alias('avg_comment')	
	)
	
	
	d25_night = d4.select('view_inc_25_night','like_inc_25_night','comment_inc_25_night').withColumn('date', lit('12-25_night')).cache()
	t5 = d25_night.groupBy('date').agg(
		
		functions.sum('view_inc_25_night').alias('sum_view'),
		functions.max('view_inc_25_night').alias('max_view'),
		functions.avg('view_inc_25_night').alias('avg_view'),
		functions.avg('like_inc_25_night').alias('avg_like'),
		functions.avg('comment_inc_25_night').alias('avg_comment')	
	)
	
	tf = tf.union(t4).union(t5).cache()
	
	tf.coalesce(1).write.option("header", "true").csv(output +'/top inc')	
if __name__ == '__main__':
	input1 = sys.argv[1]
	
	input2= sys.argv[2]
	output = sys.argv[3]
	spark = SparkSession.builder.appName('youtube pull data clearing').getOrCreate()
	assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main(input1, input2,output)
	
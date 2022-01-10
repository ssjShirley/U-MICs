#!/usr/bin/env python3
#Running method: spark-submit playList_ELT.py youtube_playlist [output]
#we have the datesets which using youtube_api.ipynb pull , give the videos music type and retuen vedio URL

import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import sys,os,uuid,gzip,re,math
from collections import Counter

from pyspark.sql import SparkSession, functions, types
from datetime import datetime
from pyspark.sql.functions import lit
from functools import reduce
from pyspark.sql.functions import expr



@functions.udf(returnType=types.IntegerType())
def getcount(count):
	return int(count)

@functions.udf(returnType=types.IntegerType())
def ishave_lofi(col):
	col = col.lower()
	if col.find('lofi') >0 or col.find('lo-fi') >0 or  col.find('lo fi') >0:
		return 1
	else:
		return 0

@functions.udf(returnType=types.IntegerType())
def ishave_hippop(col):
	col = col.lower()
	if col.find('hiphop') >0 or col.find('hip-hop') >0 or  col.find('hip hop') >0:
		return 1
	else:
		return 0

@functions.udf(returnType=types.IntegerType())
def ishave(col, word):
	col = col.lower()
	if col.find(word) >0 :
		return 1
	else:
		return 0
	
@functions.udf(returnType=types.IntegerType())
def notCon(total):
	if total==0:
		return 1
	else:
		return 0
	
@functions.udf(returnType=types.StringType())
def returnURL(video_id):
	return 'https://www.youtube.com/watch?v=' +video_id
	

def main(inputs,output):
# read table from csv file
	playlist = spark.read.option("multiline", "true")\
		.option("quote", '"')\
		.option("header", "true")\
		.option("escape", "\\")\
		.option("escape", '"').csv(inputs).repartition(40).cache()
	
	tags = playlist.select('video_tags').rdd\
		.flatMap(lambda x: x[0].replace("[","").replace("]","").lower().split(", ")).coalesce(1)
	#print(Counter(tags.collect()))
	
	d1 = playlist.select('video_id' , ishave_lofi('video_tags').alias('lofi'),ishave_hippop('video_tags').alias('hiphop'), ishave('video_tags',lit('relax')).alias('relax'),
		ishave('video_tags',lit('chill')).alias('chill'),
		ishave('video_tags',lit('study')).alias('study'),
		ishave('video_tags',lit('jazz')).alias('jazz'),
		ishave('video_tags',lit('beat')).alias('beat'),
		ishave('video_tags',lit('sleep')).alias('sleep'),
		ishave('video_tags',lit('meditation')).alias('meditation'),
		ishave('video_tags',lit('lounge')).alias('lounge'),
		ishave('video_tags',lit('cafe')).alias('cafe'),
		ishave('video_tags',lit('piano')).alias('piano'),
		
	).withColumn('total', expr('lofi+hiphop+relax+chill+study+jazz+beat+sleep+meditation+lounge+cafe+piano')).cache()
	
	d1 = d1.withColumn('other' , notCon('total')).drop('total').withColumn('URL',returnURL('video_id'))
    
	df = playlist.join(d1,'video_id')
	df.coalesce(1).write.option("header", "true").csv(output)
	
if __name__ == '__main__':
	inputs = sys.argv[1]
	
	output = sys.argv[2]
	spark = SparkSession.builder.appName('youtube pull data clearing').getOrCreate()
	#assert spark.version >= '3.0' # make sure we have Spark 3.0+
	spark.sparkContext.setLogLevel('WARN')
	sc = spark.sparkContext
	main( inputs,output)
	
# script to do simple processing of reddit data
import json
from sets import Set
import sys
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app"))
sc = SparkContext(conf = conf)

filename = sys.argv[1]
f = sc.textFile(filename)

authorCount = f.map(lambda line : json.loads(line)) \
	.filter(lambda record: 'artistID' in record) \
	.groupBy(lambda record: record['artistID']) \
	.count()

print('*************** artistCount **********************    ')
print(authorCount)

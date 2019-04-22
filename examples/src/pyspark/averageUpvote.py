import json
import sys
from pyspark import SparkConf, SparkContext

conf = (SparkConf()
         .setMaster("local")
         .setAppName("My app"))

sc = SparkContext(conf = conf)
filename = sys.argv[1]
f = sc.textFile(filename)

avg = f.map(lambda line : json.loads(line)) \
	.filter(lambda record: 'weight' in record) \
	.map(lambda record: record['weight']) \
	.mean()

print("******************************* weight *************************       ")
print(avg)

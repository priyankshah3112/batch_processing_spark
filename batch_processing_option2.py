from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
conf = SparkConf()
conf.set("spark.cassandra.connection.host", "192.168.15.87")
sc = CassandraSparkContext("spark://192.168.15.87:7077", "Simple App",conf=conf)
rdd= sc.cassandraTable("testkeyspace","stock").select("ric","date","time","high","low").groupBy(lambda r: r["date"]>20050613 and r["date"]<20170511).collect()

for gr in rdd:
	if gr[0]:
		new_rdd=sc.parallelize(list(gr[1]))

for time in ["9:30:00 AM","10:30:00 AM","11:30:00 AM","12:30:00 PM","1:30:00 PM","2:30:00 PM"]:
	rdd_temp=new_rdd.groupBy(lambda r:r["time"]==time) 
	for r in rdd_temp.collect():
		if r[0]:
			for i in r[1]:
				print(i) #each batch 
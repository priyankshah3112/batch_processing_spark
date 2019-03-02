from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf
conf = SparkConf()
conf.set("spark.cassandra.connection.host", "192.168.15.87")
sc = CassandraSparkContext("spark://192.168.15.87:7077", "Simple App",conf=conf)
rdd= sc.cassandraTable("testkeyspace","stock2").select("ric","time_stamp","high","low").spanBy('time_stamp').collect()


for gr in rdd:
	print(gr)  #one batch
	print("+++++++++++++++")
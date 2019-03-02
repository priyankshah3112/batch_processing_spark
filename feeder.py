from multiprocessing import Event,Process,Queue
import pandas as pd
import time
import pytz
from pyspark_cassandra import CassandraSparkContext
from pyspark import SparkConf, SparkContext
from datetime import datetime
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.functions import collect_list,struct,udf,first
from pyspark.sql.types import StringType


def analytical(e1,e2,q):
	
	e1.clear()
	w2=Process(target=feeder,args=(5,10,e1,e2,q))
	w2.start()

	for i in range(0,10000000): #need to create a condition to break this loop
		e2.wait()
		element = q.get()
		print(element)			#this is one batch retrieved from the queue
		time.sleep(3) 			# random processing time for analytical engine
		e1.set()

		

def feeder(start_date,end_date,e1,e2,q):

	conf = SparkConf().setAppName("Simple App").setMaster("spark://127.0.0.1:7077").set("spark.cassandra.connection.host", "127.0.0.1")

	sc = CassandraSparkContext(conf=conf)
	spark= SparkSession(sc)
	a=""
	l=['"SP1"','"SP2"']
	asia=pytz.timezone("Asia/Kolkata")
	
	#creating a dataframe for the date range and ric names 
	rdd = sc.cassandraTable("testkeyspace","stock_test").select("ric","time_stamp","high","low").where("ric in ?",["SP1","SP2","SP3"]).where("time_stamp > ? and time_stamp < ?", datetime(2010, 11, 26,12, 30, tzinfo=asia),datetime(2010, 12, 10,12, 30, tzinfo=asia) ).toDF()
	# making a batch according to the time_stamp
	rdd=rdd.orderBy("time_stamp").groupBy("time_stamp").agg(collect_list(struct('ric','time_stamp','high','low'))).collect()
	# sending one batch to analytical engine
	for gr in rdd:
		e2.clear()
		send=gr[1]
		q.put(send) #adding the batch to the queue
		e2.set()
		e1.wait()
		

if __name__ =='__main__':
	e1=Event()
	e2=Event()

	q=Queue()
	
	w1=Process(target=analytical,args=(e1,e2,q))

	w1.start()
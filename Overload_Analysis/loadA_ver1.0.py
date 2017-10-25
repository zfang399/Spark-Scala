import csv
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.fpm import FPGrowth

#read the data
load_csv=open("load_weather_summary.csv","r")
csv_reader=csv.reader(load_csv)
data=[]
ddata=[]
for item in csv_reader:
	data.append(item)

#format the data
data.remove(data[0])
for item in data:
	#remove the date
	item.remove(item[0])
	#use hour as the time unit
	item[0]="time"+str(int(item[0])/100)
	#use 10 degrees as the wind direction unit
	item[1]="dir"+str(int(item[1])/10)
	#leave wind speed,precip,temper and humid as they are
	item[2]="speed"+item[2]
	item[3]="precip"+item[3]
	item[4]="temp"+item[4]
	item[5]="hu"+item[5]
	#turn pressure into integers
	item[6]="pre"+str(int(float(item[6])))
	#remove cloud for now
	item.remove(item[7])
	#leave weekday and holiday as they are
	item[7]="day"+item[7]
	#item.remove(item[7])
	item[8]="holi"+item[8]
	#item.remove(item[7])
	#item.remove(item[7])
	ddd=float(item[9])
	item.remove(item[9])
	#print(item)
	#if ddd>14000:
	#	ddata.append(item)

#spark
sc=SparkContext("local","testing")
mdata=sc.parallelize(data)
mdata.cache()
model=FPGrowth.train(mdata,0.1,20)
print(sorted(model.freqItemsets().collect()))
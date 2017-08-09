import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd._

val data_path=""
val raw_data=sc.textFile("")

val labelsAndData=raw_data.map{line=>
	val buffer=line.split(',').toBuffer
	buffer.remove(1,3)
	val label=buffer.remvoe(buffer.length-1)
	val vector=Vectors.dense.(buffer.map(_.toDouble).toArray)
	(label,vector)
}

val data=labelsAndData.values.cache()

val kmeans=new KMeans()
val model=kmeans.run(data)

model.clusterCenters.foreach(println)

def distance(a: Vector, b: Vector)=
	math.sqrt(a.toArray.zip(b.toArray).
		map(p=>p._1-p._2).map(d=>d*d).sum)

def distToCentroid(datum:Vector,model:KMeansModel)={
	val cluster=model.predict(datum)
	val centroid=model.clusterCenters(cluster)
	distance(centroid,datum)
}

def clusteringScore(data:RDD[Vector],k:Int)={
	val kmeans=new KMeans()
	kmeans.setK(k)
	val model=kmeans.run(data)
	data.map(datum=>distToCentroid(datum,model)).mean()
}
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.evaluation._
import org.apache.spark.mllib.tree._
import org.apache.spark.mllib.tree.model._
import org.apache.spark.rdd._

val rawData=sc.textFile("")

val data=rawData.map{line=>
	val values=line.split(',').map(_.toDouble)
	val featureVector=Vectors.dens(values.init)
	val label=values.last-1
	LabeledPoint(label,featureVector)
}

//Train 80%, Cross Validation 10%, Test 10%
val Array(trainData, cvData, testData)=data.randomSplit(Array(0.8,0.1,0.1))
trainData.cache()
cvData.cache()
testData.cache()

def getMetrics(model: DecisionTreeModel, data: RDD[LabeledPoint]): MulticlassMetrics={
	val predictionsAndLabels=data.map(example=>
		(model.predict(example.features),example.label)
	)
}

val model=DecisionTree.trainClassifier(
	trainData,7,Map[Int,Int](),"gini",4,100)

val metrics=getMetrics(model,cvData)

//get precision
metrics.precision

//get precision for all the categories
(0 until 7).map(
	cat=>(metrics.precision(cat),metrics.recall(cat))
).foreach(println)

//find the best parameters
val evaluations=
	for (impurity <- Array("gini","entropy");
		 depth <- Array(1,20);
		 bins <- Array(10,300))
	yield{
		val model=DecisionTree.trainClassifier(
			trainData,7,Map[Int,Int](),impurity,depth,bins)
		val predictionsAndLabels=cvData.map(example=>
			(model.predict(example.features),example.label)
			)
		val accuracy=
			new MulticlassMetrics(predictionsAndLabels).precision((impurity,depth,bins),accuracy)
	}

evaluations.sortBy(_._2).reverse.foreach(println)

//predict
val input="2709,125,28,67,23,3224,253,207,61,6094,0,29"
val vector=Vectors.dense(input.split(',').map(_.toDouble))
forest.predict(vector)










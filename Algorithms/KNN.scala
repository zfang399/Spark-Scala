import scala.collection.mutable.Map

//learned from http://blog.csdn.net/u011239443/article/details/63254084

object KNN{
	def getGroup(): Array[Array[Double]]={
		return Array(Array(1.0,1.1),Array(1.0,1.0),Array(0,0),Array(0,0.1))
	}

	def getLabels(): Array[Char]={
		return Array('A','A','B','B')
	}

	def clf(inX: Array[Double], dataSet:Array[Array[Double]], labels:Array[Char], k:Int): Char={
		val dataSetSize = dataSet.length
	    val sortedDisIndicies = dataSet.map { x =>
	      val v1 = x(0) - inX(0)
	      val v2 = x(1) - inX(1)
	      v1 * v1 + v2 * v2
	    }.zipWithIndex.sortBy(f => f._1).map(f => f._2)
	    var classsCount: Map[Char, Int] = Map.empty
	    for (i <- 0 to k - 1) {
	      val voteIlabel = labels(sortedDisIndicies(i))
	      classsCount(voteIlabel) = classsCount.getOrElse(voteIlabel, 0) + 1
	    }
	    classsCount.toArray.sortBy(f => -f._2).head._1
	}

	def main(args: Array[String]) {
    	println(clf(Array(0, 0), getGroup(), getLabels(), 3))
    }
}
def dijkstrap[VD](g:Graph[VD,Double],origin:VertexId):Graph[(VD,Double),Double]={
	import org.apache.spark.graphx._

	//initialize
	var gg=g.mapVertices{
		case(vid,_) =>
			val vd=if(vid==origin) 0 else Double.MaxValue
			(false,vd)
	}

	//loop through all the points
	(0L until g.vertices.count).foreach{i:Long =>
		//find the current vertex id
		val currentVertexId=gg.vertices.filter(!_._2._1)
			.fold((0L,(false,Double.MaxValue))){
				case(a,b) => if (a._2._2 < b._2._2) a else b
			}._1

		val newDistances: VertexRDD[Double]=gg.aggregateMessages[Double](
			ctx => if(ctx.srcId==currentVertexId) ctx.sendToDst(ctx.srcAttr._2+ctx.attr),
			(a,b) => math.min(a,b)
		)

		gg=gg.outerJoinVertices(newDistances){(vid,vd,newSum) =>
			(vd._1 || vid==currentVertexId,math.min(vd._2,newSum.getOrElse(Double.MaxValue)))
		}
	}

	g.outerJoinVertices(gg.vertices){(vid,vd,dist) =>
		(vd,dist.getOrElse((false,Double.MaxValue))._2)
	}
}
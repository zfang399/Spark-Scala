def minSpanningTree[VD: scala.reflect.ClassTag](g:Graph[VD,Double])={
	var gg=g.mapEdges(e=>(e.attr,false))
	for(i<-1L to g.vertices.count-1){
		val unavailableEdges=gg.outerJoinVertices(gg.subgraph(_.attr._2)
			.connectedComponents
			.vertices)((vid,vd,cid) => (vd,cid))
			.subgraph(et => et.srcAttr._2.getOrElse(-1) == et.dstAttr._2.getOrElse(-2))
			.edges
			.map(e => ((e.srcId,e.dstId),e.attr))

		type edgeType=((VertexId,VertexId),Double)
		val smallestEdge=gg.edges
			.map(e => ((e.srcId,e.dstId),e.attr))
	}
}
import scala.io.Source
import java.io
import java.util.Random

object Kmeans{
	val k = 5 				
	val d = 30				
	val thresh=0.00000001	
	val centers = new Array[Vector[Double]] (k)

	def main(args:Array[String]){
		val data_path="root/testkmeans.txt"
		val lines=Source.fromFile(fileName).getLines()
		val data=lines.map(line =>{
			val parts=line.split(" ").map(_.toDouble)
			val res=Vector[Double]()
			for (i<-0 to d-1) res ++= Vector(parts(i))
			res
		}).toArray

		ini(data)
		kmeans(data,centers)
		printres(data,centers)
	}

	def ini(data:Array[Vector[Double]])={
		val ran=new Random(System.currentTimeMillis())
		for (i<-0 to k-1) centers(i)=data(rand.nextInt(data.length)-1)
	}

	def calcdis(p:Vector[Double],q:Vector[Double])={
		var ret=0
		for(i<-0 to d-1){
			ret=ret+(p(i)-q(i))*(q(i)-q(i))
		}
		math.sqrt(ret)
	}

	def calcsum(p:Vector[Double],q:Vector[Double])={
		val pa=p.toArray
		val qa=q.toArray
		var ret=Vector[Double]()
		for(i<-0 to pa.length-1){
			ret++=Vector(pa(i)+qa(i))
		}
		ret
	}

	def calcdiv(p:Vector[Double],tot:Int)={
		val pa=p.toArray
		var ret=Vector[Double]()
		for(i<-0 to pa.length-1){
			ret++=Vector(pa(i)/tot)
		}
		ret
	}

	def findcenter(centers:Array[Vector[Double]],p:Vector[Double]): Vector[Double] ={
		centers.reduceLeft(x,y)=>if(calcdis(x,p) < calcdis(y,p)) x else y
	}

	def kmeans(data:Array[Vector[Double]],centers:Array[Vector[Double]])={
		var finished=false
		while(!finished){
			val c=data.groupBy{findcenter(centers,_)}

			val newcenters=centers.map{t=>
				c.get(t) match{
					case Some(p) => calcdiv(p.reduceLeft(calcsum(_,_)),p.length)
					case None => t
				}
			}

			var movedis=0
			for(i<-0 to k-1){
				movedis+=math.sqrt(calcdis(centers(i),newcenters(i)))
				centers(i)=newcenters(i)
			}
			if(movement<=thresh) finished=true
		}
	}

	def printres(data:Array[Vector[Double]],ceters:Array[Vector[Double]])={
		val labels=new Array[Int](data.length)
		for(i<-0 to data.length-1){
			val tmp=centers.reduceLeft((a,b)=> if((calcdis(a,data(i)))<(calcdis(b,data(i)))) a else b)
			labels(i)=centers.indexOf(tmp)
			println(labels(i))
		}
	}
}
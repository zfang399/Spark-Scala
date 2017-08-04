import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import java.util.BitSet
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark._

object apriori {
  //main
  def main(args: Array[String]): Unit = {
    //initialize SparkContext and parameters
    val sc=new SparkContext()
    val tot_num=17974836
    val sup_num=15278611
    val K=8

    //remove transaction ids, combine the same transactions
    val trans=sc.textFile(args(0)).map(line =>
      line.substring(line.indexOf(" ")+1).trim).map((_,1)).reduceByKey(_+_).map(line=> {
      val bitSet=new BitSet()
      val ts=line._1.split(" ")
      for(i <- 0 until ts.length){
        bitSet.set(ts(i).toInt,true)
      }
      (bitSet,line._2)
    }).cache()

    //Get 1 frequent item set
    var fi=trans.flatMap{ line =>
      val tmp=new ArrayBuffer[(String,Int)]
      for(i<-0 until line._1.size()){
        if(line._1.get(i)) tmp+=((i.toString(),line._2))
      }
      tmp
    }.reduceByKey(_+_).filter(l => l._2>=sup_num).cache()
    val res=fi.map(line => line._1+":"+(line._2/tot_num))
    res.saveAsTextFile(args(1) + "/result-1")

    //loop to get 2 to K frequent item set
    for(i<-2 to K){
      val canfi=getCanFI(fi.map(_._1).collect,i)
      val bccFI=sc.broadcast(canfi)
      //update fi to be i frequent item set
      fi=trans.flatMap{ line =>
        var tmp=new ArrayBuffer[(String,Int)]()
        bccFI.value.foreach{itemset=>
          val itemArray=itemset.split(",")
          var count=0
          for (item <- itemArray) if(line._1.get(item.toInt)) count+=1
          if (count==itemArray.size) tmp+=((itemset,line._2))
        }
        tmp
      }.reduceByKey(_+_).filter(_._2>=sup_num).cache()
      val res=fi.map(line => line._1+":"+(line._2/tot_num))
      res.saveAsTextFile(args(i)+"/result-"+i)
      bccFI.unpersist()
    }
  }

  //Get candidate K frequent item set from K-1 frequent item set
  def getCanFI(fi: Array[String], tag:Int) = {
    val arrayBuffer=ArrayBuffer[String]()
    for(i<-0 until fi.length;j<-i until fi.length){
      var tmp=""
      if(tag==2){
        tmp=(fi(i)+","+fi(j)).split(",").sortWith((a,b)=>a.toInt<=b.toInt).reduce(_+","+_)
      }else{
        if(fi(i).substring(0,fi(i).lastIndexOf(",")).equals(fi(j).substring(0,fi(j).lastIndexOf(",")))){
          tmp=(fi(i).substring(0,fi(i).lastIndexOf(","))+fi(j).substring(fi(j).lastIndexOf(","))).split(",").sortWith((a,b)=>a.toInt<=b.toInt).reduce(_+","+_)
        }
      }
      var hasinfreq=false
      if(tmp==""){
        hasinfreq=true
      }else{
        val arrayTmp=tmp.split(",")
        breakable {
          for(i<-0 until arrayTmp.size){
            var subItem=""
            for(j<-0 until arrayTmp.size){
              if(i!=j) subItem+=arrayTmp(j)+","
            }
            subItem=subItem.substring(0,subItem.lastIndexOf(","))
            if(!fi.contains(subItem)){
              hasinfreq=true
              break
            }
          }
        }
      }
      if(!hasinfreq) arrayBuffer+=(tmp)
    }
    arrayBuffer.toArray
  }
}

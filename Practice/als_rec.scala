import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib._
import org.apache.spark.mllib.recommendation._

object als_rec {
  def main(args:Array[String])={
    val conf=new SparkConf().setMaster("local").setAppName("ALS")
    val sc=new SparkContext(conf)

    //get data
    val data_path="/Users/zhaoyuanfang/desktop/fpg.txt"
    val raw_data=sc.textFile(data_path)

    //make model
    val train_data=raw_data.map{line=>
      val Array(userID,productID,count)=line.split(' ').map(_.toInt)
      Rating(userID,productID,count)
    }
    val model=ALS.trainImplicit(train_data,6,5,0.01,1.0)

    val recommendations=model.recommendProducts(6,5)
    recommendations.foreach(println)
  }
}

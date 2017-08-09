import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.mllib.recommendation._

conf=new SparkConf()
sc=new SparkContext(conf)

//get user-artist data
data_path="user_artist_data.txt"
val user_artist_data=sc.textFile(data_path)

//get artist data
artist_data_path="artist_data.txt"
val raw_data=sc.textFile(artist_data_path)
val artistByID=raw_data.flatMap{line =>
	val (id,name)=line.span(_!='\t')
	if (name.isEmpty){
		None
	}else{
		try{
			Some((id.toInt,name.trim))
		}catch{
			case e:NumberFormatException=>None
		}
	}
}

//get rid of the different versions of the same artist
artist_alias_path="artist_alias.txt"
val raw_artist_alias=sc.textFile(artist_alias_path)
val artist_alias=raw_artist_alias.flatMap{line =>
	val tokens=line.split('\t')
	if(tokens(0).isEmpty){
		None
	}else{
		Some((tokens(0).toInt,tokens(1).toInt))
	}
}.collectAsMap()

//make model
val b_artist_alias=sc.broadcast(artist_alias)
val train_data=user_artist_data.map{line=>
	val Array(userID,artistID,count)=line.split(' ').map(_.toInt)
	val finalartistID=b_artist_alias.value.getOrElse(artistID,artistID)
	Rating(userID,finalartistID,count)
}.cache()

val model=ALS.trainImplicit(train_data,10,5,0.01,1.0)

//check the results for certain user
val raw_artists_for_user=user_artist_data.map(_.split(' ')).
	filter{case Array(user,_,_)=>user.toInt==2093760}
val existingProducts=raw_artists_for_user.map{
	case Array(_,artist,_)=>artist.toInt
}.collect.toSet

artistByID.filter{case(id,name)=>
	existingProducts.contains(id)
}values.collect().foreach(println)

val recommendations=model.recommendProducts(2093760,5)
recommendations.foreach(println)

val recommendationsProductIDs=recommendations.map(_.product).toSet
artistByID.filter{case(id,name)=>
	recommendationsProductIDs.contains(id)
}values.collect().foreach(println)
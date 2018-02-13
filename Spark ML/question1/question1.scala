import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

val data = sc.textFile("itemusermat")
val dataMap = data.map(s => Vectors.dense(s.split(' ').drop(1).map(_.toDouble))).cache()
val numOfClusters = 10
val iterations = 100
val kMeansModel = KMeans.train(dataMap, numOfClusters, iterations)

val pred = data.map{x =>(x.split(' ')(0), kMeansModel.predict(Vectors.dense(x.split(' ').drop(1).map(_.toDouble))))}

val movie = sc.textFile("movies.dat")
val moviesData = movie.map(x => (x.split("::"))).map( p => ( p(0),(p(1)+", "+p(2))))


val res = pred.join(moviesData)
val group = res.map(x => (x._2._1, (x._1, x._2._2))).groupByKey()

val result = group.map(x => (x._1,x._2.toList))

val output = result.map(x => (x._1,x._2.take(5)))
println("Cluster Id , List of first 5 Movies in cluster")
output.foreach( x => println("Cluster: " + x._1 + " \n " + x._2.mkString("\n")))
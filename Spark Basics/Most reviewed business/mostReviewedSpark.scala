val dataBusiness = sc.textFile("business.csv").cache().map(line => line.split("::")).map(line => (line(0),line))

val dataReview = sc.textFile("review.csv").cache().map(line => line.split("::")).map(line => (line(2)))

val count = dataReview.map(x => (x,1)).reduceByKey(_+_).map(x => (x._1,x._2)).sortBy(_._2,false).take(10)

val countRDD = sc.parallelize(count)

val joined = countRDD.distinct().join(dataBusiness).values

val mostReviewedBusiness = joined.map(x => (x._2.mkString("\t"),x._1)).distinct().sortBy(_._2,false)

val mostReviewedBusiness1 = mostReviewedBusiness.map(x => (x._1 + "\t" +x._2))

mostReviewedBusiness1.repartition(1).saveAsTextFile("MostReviewedSparkOutput");
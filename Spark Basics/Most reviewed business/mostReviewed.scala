val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val dataBusiness = sc.textFile("business.csv").cache().map(line => line.split("::")).map(line => (line(0),line(1),line(2))).toDF("b_id","address","categories")

val dataReview = sc.textFile("review.csv").cache().map(line => line.split("::")).map(line => (line(1), line(2))).toDF("u_id", "b_id");

dataReview.registerTempTable("review")

val reviewData = sqlContext.sql("select b_id,count(*) as total from review group by b_id")

val dFReview = reviewData.toDF("b_id","count")

dataBusiness.createOrReplaceTempView("business")

dFReview.createOrReplaceTempView("review")

val joinedTable = sqlContext.sql("SELECT business.b_id, business.address,business.categories,review.count FROM review JOIN business ON business.b_id = review.b_id group by business.b_id, business.address,business.categories,review.count order by review.count desc limit 10").rdd.cache()

val mostReviewdBusiness = joinedTable.map(x => (x.get(0) + "\t" + x.get(1) + "\t" + x.get(2) + "\t" + x.get(3)))

mostReviewdBusiness.repartition(1).saveAsTextFile("mostReviewedSQLOutput");
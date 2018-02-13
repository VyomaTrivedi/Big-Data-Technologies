val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val dataBusiness = sc.textFile("business.csv").cache().map(line => line.split("::")).map(line => (line(0),line(1))).toDF("b_id","address")

dataBusiness.registerTempTable("business")
val data = sqlContext.sql("select distinct b_id from business where address like '%Palo Alto%'")

val dataReview = sc.textFile("/Users/vyomatrivedi/Documents/Big data/project2/review.csv").cache().map(line => line.split("::")).map(line => (line(0), line(1), line(2), line(3))).toDF("r_id", "u_id", "b_id", "stars");

data.createOrReplaceTempView("business")

dataReview.createOrReplaceTempView("review")

val joinedTable = sqlContext.sql("SELECT review.u_id, review.stars FROM review JOIN business ON business.b_id = review.b_id").rdd.cache()

val ans = joinedTable.map(x => ((x.get(0).toString().replace("(","").replace(")","")+"\t"+x.get(1))))

ans.repartition(1).saveAsTextFile("PaloAltoSQLOutput");
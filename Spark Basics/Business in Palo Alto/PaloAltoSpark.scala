val dataBusiness = sc.textFile("/Users/vyomatrivedi/Documents/Big data/project2/business.csv").cache()

val dataReview = sc.textFile("/Users/vyomatrivedi/Documents/Big data/project2/review.csv").cache()


val filteredReview = dataReview.map(line => line.split("::")).map(line => (line(2), line));   //bid review data

val filteredBusiness = dataBusiness.filter(line => line.contains("Palo Alto")).map(line => line.split("::")).map(line => (line(0), line(0)));  // bid bid

val joinedTable = filteredBusiness.distinct().join(filteredReview).values.values.map(line => (line(1), line(3)));

val answer = joinedTable.map(x => ((x._1.toString().replace("(","").replace(")","")+"\t"+x._2)))
answer.saveAsTextFile("PaloAltoSparkOutput");
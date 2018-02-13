val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val rdd = sc.textFile("/Users/vyomatrivedi/Documents/Big data/project2/test.txt").cache()
var userToFriendsRdd = rdd.filter(line => (line.split("\t").size > 1)).map(line => line.split("\t")).map(tokens => (tokens(0),tokens(1).split(",").filter(!_.isEmpty).toList))

def createPair(line: String): Array[(String, String)] = {
  val splits = line.split("\t")
  val kuid = splits(0)
  if (splits.size > 1) {
    splits(1).split(",").map {
      segment => {
        if (segment.toLong < kuid.toLong) {
          (segment, kuid)
        } else {
          (kuid, segment)
        }
      }
    }
  } else {
    Array(null)
  }
}

val pair = rdd.flatMap { line => createPair(line) }.distinct()

val pairs = pair.filter(x => x != null)

val reversedPairs = pairs.map(x => (x._2, x._1))

val pairsDF = pairs.toDF("user1","user2")

val userFriends = userToFriendsRdd.toDF("user","friends")

pairsDF.registerTempTable("pairs")

userFriends.registerTempTable("friends")

val user1Friends = sqlContext.sql("select user1,user2,friends from pairs join friends on pairs.user1 = friends.user")

val userMFriends = sqlContext.sql("select user1,user2,friends from pairs join friends on pairs.user2 = friends.user")

val user1FriendsList = user1Friends.toDF("user1","user2","f1")

val user2FriendsList = userMFriends.toDF("user1","user2","f2")

user1FriendsList.registerTempTable("user1FriendsList")

user2FriendsList.registerTempTable("user2FriendsList")

val mutual = sqlContext.sql("select user1FriendsList.user1,user1FriendsList.user2,f1,f2 from user1FriendsList join user2FriendsList on user1FriendsList.user1 = user2FriendsList.user1 and user1FriendsList.user2 = user2FriendsList.user2")

val mutualDF = mutual.toDF("user1","user2","friend1List","friend2List")

mutualDF.registerTempTable("mutual")

spark.udf.register("array_intersect",(xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size)

val mutualFriends = sqlContext.sql("select user1,user2,array_intersect(friend1List,friend2List) as count from mutual order by count desc limit 10")

val mutualFriendsDF = mutualFriends.toDF("user1","user2","count")

mutualFriendsDF.registerTempTable("mutualFriends")

val userRdd = sc.textFile("/Users/vyomatrivedi/Documents/Big data/project2/userdata.txt")

val userDetails = userRdd.map(line => line.split(",")).map(x => (x(0),x(1),x(2),x(3)))

val userData = userDetails.toDF("id","firstname","lastname","address")

userData.registerTempTable("userData")

val user1Data = sqlContext.sql("select user1,user2,count,firstname,lastname,address from mutualFriends join userData on mutualFriends.user1 = userData.id")

val user2Data = sqlContext.sql("select user1,user2,count,firstname,lastname,address from mutualFriends join userData on mutualFriends.user2 = userData.id")

val table1 = user1Data.toDF("user1","user2","count","firstname","lastname","address")

val table2 = user2Data.toDF("user1","user2","count","firstname","lastname","address")

table1.registerTempTable("table1")

table2.registerTempTable("table2")

val ans = sqlContext.sql("select table1.user1,table1.user2,table1.count,table1.firstname,table1.lastname,table1.address," +
  "table2.firstname,table2.lastname,table2.address " +
  "from table1,table2 where table1.user1 = table2.user1 and table1.user2 = table2.user2")

val outputAnswer = ans.rdd.map(x => (x.get(2) + "\t" + x.get(3) + "\t" + x.get(4) + "\t" + x.get(5)+ "\t" + x.get(6) + "\t" + x.get(7) + "\t" + x.get(8)))

outputAnswer.repartition(1).saveAsTextFile("Mutual2SQLOutput")
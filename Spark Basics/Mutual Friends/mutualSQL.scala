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

val mutualFriends = sqlContext.sql("select user1,user2,array_intersect(friend1List,friend2List) as count from mutual")

val mutualRDD= mutualFriends.rdd.map(x => ((x.get(0),x.get(1)).toString(),x.get(2)))

val mutualFriendsOfUsers = mutualRDD.map(x => x._1.toString().replace("(","").replace(")","")+"\t"+x._2)

mutualFriendsOfUsers.repartition(1).saveAsTextFile("MutualSQLOutput")
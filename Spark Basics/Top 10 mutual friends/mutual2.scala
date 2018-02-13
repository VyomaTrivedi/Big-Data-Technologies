
val rdd = sc.textFile("test.txt").cache()

val userRdd = sc.textFile("userdata.txt").cache()

var userToFriendsRdd = rdd.filter(line => (line.split("\t").size > 1)).map(line => line.split("\t")).map(tokens => (tokens(0), tokens(1).split(",").filter(!_.isEmpty).toList))

val userDetails = userRdd.map(line => line.split(",")).map(x => (x(0),x(1),x(2),x(3)))

def createPair(line: String): Array[(String, String)] = {
  val splits = line.split("\t")
  val kuid = splits(0)
  if (splits.size > 1) {
    splits(1).split(",").map {
      segment => {
        if (segment < kuid) {
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

val friendList = userToFriendsRdd.map(x => (x._1, x))

val user1FriendList = pairs.join(friendList).cache()

val user2FriendList = reversedPairs.join(friendList).cache()

val user1Friends = user1FriendList.map(x => (x._1, x._2._1, x._2._2._2))

val user2Friends = user2FriendList.map(x => (x._2._1, x._1, x._2._2._2))

val user1user2Friends = user1Friends.map {
  case (key1, key2, value) => ((key1, key2), value)
  }.join(
    user2Friends.map {
      case (key1, key2, value) => ((key1, key2), value)
      })
  val mutualFriends = user1user2Friends.map(x => (x._1,x._2._1.intersect(x._2._2).size))

  val topMutualFriends = mutualFriends.sortBy(_._2,false)

  val topMutualFriends1 =topMutualFriends.map(y => (y._1._1,y._1._2,y._2))

  val users = topMutualFriends.map(x => (x._1._1,x._1._2))

  val usersReversed = topMutualFriends.map(x => (x._1._2,x._1._1))

  val userDetailsJoinData = userDetails.map(x => (x._1,x))

  val user1DetailsJoin = users.join(userDetailsJoinData)

  val user1Details = user1DetailsJoin.map(x => (x._1, x._2._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))

  val user2DetailsJoin = usersReversed.join(userDetailsJoinData)

  val user2Details = user2DetailsJoin.map(x => (x._2._1, x._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))

  val user1user2DetailsJoin = user1Details.map {
    case (key1, key2, value) => ((key1, key2), value)
    }.join(
      user2Details.map {
        case (key1, key2, value) => ((key1, key2), value)
        })

    val tp = user1user2DetailsJoin.map(x => (x._1._1,x._1._2,x))

    val joinedTable = topMutualFriends1.map {
      case (key1, key2, value) => ((key1, key2), value)
      }.join(
        tp.map {
          case (key1, key2, value) => ((key1, key2), value)
          })

      val answers = joinedTable.map(d => (d._1._1,d._1._2,d._2._1,d._2._2._2._1,d._2._2._2._2))

      val sorted= answers.sortBy(_._3,false).zipWithIndex.filter{case (_, idx) => idx < 11}.keys

      val sortedOutput= sorted.map(x=> (x._3.toString + "\t" + x._4.toString().replace(",","\t").replace("(","").replace(")","") + "\t\t\t" + x._5.toString().replace(",","\t").replace("(","").replace(")","")))

      sortedOutput.repartition(1).saveAsTextFile("Mutual2SparkOutput");

      
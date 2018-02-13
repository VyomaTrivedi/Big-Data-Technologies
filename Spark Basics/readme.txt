To run the code unzip the file to extract the folders for each question.

Then using command line go to the location of the extracted folder
For example : cd /Users/vyomatrivedi/Documents/Big data/project2/1

Then run the following command : spark-shell -i fileName

For example : spark-shell -i MutualSpark.scala

Once the command executes successfully the output will be stored in the same folder(1 in case of our example)


Output Formats and commands
————————————————————————————————————————————————————————————

1 : Find mutual friends using spark and spark sql
user1,user2	Number of Mutual Friends

spark-shell -i mutualSpark.scala			(using Spark)
spark-shell -i mutualSQL.scala				(using Spark SQL and Dataframe)

2 : Find top 10 mutual friends using spark and spark sql
Total number of Common Friends	First Name of User A	LastName of User A<TAB>address of User A	First Name of User B	Last Name of User B	address of User B

spark-shell -i mutual2.scala				(using Spark)
spark-shell -i mutual2SQL.scala				(using Spark SQL and Dataframe)

3 : Find most reviewed business from given data using spark and spark sql
business_id	full_address	categories	number_rated

spark-shell -i mostReviewed.scala			(using Spark SQL and Dataframe)
spark-shell -i mostReviewedSpark.scala			(using Spark)

4 : Using given data find business in Palo Alto using spark and spark sql
User_id	Rating

spark-shell -i PaloAltoSpark.scala			(using Spark)
spark-shell -i PaloAltoSQL.scala			(using Spark SQL and Dataframe)
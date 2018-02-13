Download the File ratings.dat 
	     File movies.dat 
             File itemusermat 
             File glass.data
And keep them in the same folder as the codes

1. Use Means to cluster movies by category using given data
run it using the following command
	
        spark-shell -i question1.scala

It will print out the clusters and any 5 movies in that cluster

2. Implement ALS on given data
run it using the folowing command
	
        spark-shell -i question2.scala

It will print the accuracy of ALS

3. Implement kmm on given training and testing files.
Download the files and rename the test train pairs as 
   train1, test1, train2, test2 and so on..
   Keep them in the same folder as the code
   
   Then run the code using the following command
	spark-submit question3.py 10

It will print the accuracy for classification using kmm and without it for all 6 pairs of training and testing file

4. Twitter analysis on presidency.
To run it download eclipse ide for Scala
	open the code in it
	replace the consumer and access token keys with he strings
	right click on question4.scala -> run configuration -> arguments
	in arguments give the path to output directory and click Run
	The analysis files will be generated in the output directory
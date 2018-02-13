import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class mutualFriends {

    public static class Map
    extends Mapper<LongWritable, Text, Text, Text>{

    	 private Text keyFriends = new Text(); 
         private Text valueFriends = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	String[] meAndFriends = value.toString().split("\t");
            String[] myFriends ;
            
            if(meAndFriends.length > 1){
               
            myFriends = meAndFriends[1].split(",");
            String[] friendsKey = new String[2];
            for(String friend : myFriends ){
               
                friendsKey[0] = meAndFriends[0];
                friendsKey[1]=friend;
           
                Long friend1 = Long.parseLong(friendsKey[0]);
                Long friend2 = Long.parseLong(friendsKey[1]);
                
                if(friend1 < friend2){
                   
                    keyFriends.set(friendsKey[0]+","+friendsKey[1]);
                }
                else{
                	
                    keyFriends.set(friendsKey[1]+","+friendsKey[0]);
                }
               
                valueFriends.set(meAndFriends[1]);
               
                context.write(keyFriends, valueFriends);
                }
            }
        }
    }

    public static class Reduce
    extends Reducer<Text,Text,Text,Text> {
       
        private Text result = new Text();
       
        private Text output1 = new Text();
        private Text output2 = new Text();
        private Text output3 = new Text();
        private Text output4 = new Text();
        private Text output5 = new Text();
        HashSet<Text> outputKeys = new HashSet<Text>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
            
            output1.set("0,4");
            output2.set("20,22939");
            output3.set("1,29826");
            output4.set("6222,19272");
            output5.set("28041,28056");
            
            outputKeys.add(output1);
            outputKeys.add(output2);
            outputKeys.add(output3);
            outputKeys.add(output4);
            outputKeys.add(output5);
            String[] friendsOfKey = new String[2];
            ArrayList<String> mutualFriendsList = new ArrayList<String>();
            String mutualFriends = "";
            int count = 0;
            for(Text value : values) {      
                friendsOfKey[count++] = value.toString();
            }
            String[] friends1 = friendsOfKey[0].split(",");
            String[] friends2 = friendsOfKey[1].split(",");
            for(String friend1 : friends1 ) {
                mutualFriendsList.add(friend1);
            }
            for(String friend2 : friends2 ) {
                if(mutualFriendsList.contains(friend2)) {
                    mutualFriends+= friend2 + ",";
                }
            }
            if(mutualFriends.length() > 0) {
                mutualFriends = mutualFriends.substring(0, mutualFriends.length() - 1);
            }
            
            
            // comment this code if the arguments need to be passed from command line
            if(outputKeys.contains(key)) {
            	result.set(mutualFriends);
            	context.write(key, result);
            }
            // comment till here
            
            //uncomment the following code to pass the input pairs from command line
//            Configuration conf = context.getConfiguration();
//            if(key.toString().equalsIgnoreCase(conf.get("findMutualFriends").toString())) {
//            	
//            	result.set(mutualFriends);
//            	context.write(key, result);
//            }
           
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // comment this if you want to pass friend input from command line
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <in> <out>");
            System.exit(2);
        }
        
        // uncomment this to pass value from command line
//        if (otherArgs.length != 3) {
//            System.err.println("Usage: MutualFriends <in> <out>");
//            System.exit(2);
//        }
//		  conf.set("findMutualFriends", otherArgs[2s]);
        
        // create a job with name "mutualFriends"
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "mutualFriends");
        job.setJarByClass(mutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
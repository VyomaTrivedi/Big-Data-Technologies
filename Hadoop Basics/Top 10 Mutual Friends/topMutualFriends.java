import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class topMutualFriends {

    public static class Map2
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
    extends Reducer<Text,Text,Text,IntWritable> {
      
        private IntWritable result = new IntWritable();
        private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable>();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String[] friendsOfKey = new String[2];
            ArrayList<String> mutualFriendsList = new ArrayList<String>();
            String mutualFriends = "";
            int count = 0;
            int numOfMutualFriends = 0;
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
                    numOfMutualFriends++;
                }
            }
            countMap.put(new Text(key), new IntWritable(numOfMutualFriends));
            result.set(numOfMutualFriends);          
        }
       
        public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
            List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

            Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

                public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            //LinkedHashMap will keep the keys in the order they are inserted
            //which is currently sorted on natural ordering
            Map<K, V> sortedMap = new LinkedHashMap<K, V>();

            for (Map.Entry<K, V> entry : entries) {
                sortedMap.put(entry.getKey(), entry.getValue());
            }

            return sortedMap;
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
       
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: topMutualFriends <in> <out>");
            System.exit(2);
        }


        // create a job with name "topMutualFriends"
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "topMutualFriends");
        job.setJarByClass(topMutualFriends.class);
        job.setMapperClass(Map2.class);
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
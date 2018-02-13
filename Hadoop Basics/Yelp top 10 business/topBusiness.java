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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class topBusiness {
	public static class MapFindTop extends Mapper<Object, Text, Text, FloatWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
        	
        	String[] data = value.toString().split("::"); 
        	
            Float rating = Float.parseFloat(data[3]);
            
            Text keyTop = new Text(data[2]);
            FloatWritable valueTop = new FloatWritable(rating);
            
            context.write(keyTop, valueTop);  
        }
    }

	public static class MapBusiness extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] data = value.toString().split("::");
			String businessKey = data[0];
			String businessValue = "Business" + "\t" + data[1] + "\t" + data[2];
            
            context.write(new Text(businessKey), new Text(businessValue));  
        }
	}
	public static class MapTop10 extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] data = value.toString().split("\t");
			String businessId = data[0];
			String averageRating = data[1];
            
            context.write(new Text(businessId), new Text(averageRating));  
        }
		
	}
    public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    	
    	private Map<Text, FloatWritable> businessRating = new HashMap<Text, FloatWritable>();
        
    	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        	float sum = 0;
        	int count = 0;
        	
        	for(FloatWritable value : values) {
        		sum += value.get();
        		count++;
        	}
        	float averageRating = sum/count;
        	
        	businessRating.put(new Text(key), new FloatWritable(averageRating));
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
        	
        		Map<Text, FloatWritable> sortedMap = sortByValues(businessRating);
        		int counter = 0;
                for (Text key : sortedMap.keySet()) {
                    if (counter++ == 10) {
                        break;
                    }    
                    context.write(key, sortedMap.get(key));
                }
        }
        
    }

    public static class reduceSideJoinReducer extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String averageRating = "";
    		String businessInfo = "";
    		boolean flag = false;
    		for(Text value : values) {
    			String data = value.toString();
    			if(data.contains("Business")) {
    				data = data.replace("Business", "");
    				businessInfo = data;
    			} else {
    				averageRating = data;
    				flag = true;
    			}
    		}
    		if(!flag)
    			return;
    		context.write(new Text(key), new Text(businessInfo + "\t" + averageRating));
    	}
    }
    // Driver program
    public static void main(String[] args) throws Exception {
    	   Configuration conf = new Configuration();
           String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
           if (otherArgs.length != 4) {
               System.err.println("Usage: topBusiness <input1> <input2> <intermideatePath> <output>");
               System.exit(2);
           }
            
           Job job1 = Job.getInstance(conf, "topBusiness");
           job1.setJarByClass(topBusiness.class);
    
           job1.setMapperClass(MapFindTop.class);
           job1.setReducerClass(Reduce.class);
    
           job1.setMapOutputKeyClass(Text.class);
           job1.setMapOutputValueClass(FloatWritable.class);
    
           job1.setOutputKeyClass(Text.class);
           job1.setOutputValueClass(FloatWritable.class);
           job1.setNumReduceTasks(1);
    
           Path reviewPath = new Path(otherArgs[0]);
           Path businessPath = new Path(otherArgs[1]);
           Path intermediatePath = new Path(otherArgs[2]);
           Path outputPath = new Path(otherArgs[3]);
    
           FileInputFormat.addInputPath(job1, reviewPath);
           FileOutputFormat.setOutputPath(job1, intermediatePath);
           job1.waitForCompletion(true);
    
           Job job2 = Job.getInstance(conf, "topBusiness");
           job2.setJarByClass(topBusiness.class);
           MultipleInputs.addInputPath(job2, businessPath, TextInputFormat.class, MapBusiness.class);
           MultipleInputs.addInputPath(job2, intermediatePath, TextInputFormat.class, MapTop10.class);
    
           job2.setReducerClass(reduceSideJoinReducer.class);
    
           job2.setOutputKeyClass(Text.class);
           job2.setOutputValueClass(Text.class);
           FileInputFormat.setMinInputSplitSize(job2, 500000000);
    
           FileSystem hdfsFS = FileSystem.get(conf);
           if (hdfsFS.exists(outputPath)) {
               hdfsFS.delete(outputPath, true);
           }
           FileOutputFormat.setOutputPath(job2, outputPath);
    
           job2.waitForCompletion(true);
    
           hdfsFS.delete(intermediatePath, true);
    }

}

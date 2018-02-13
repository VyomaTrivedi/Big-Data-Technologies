import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
 
import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class findPaloAlto {
 
    public static class MapFilter extends Mapper<Object, Text, Text, Text> {
        Set<String> criteriaBusinessId = new HashSet<String>();
        String filterCriteria;
 
        public void setup(Context context) throws IOException, InterruptedException {
             
            Configuration conf = context.getConfiguration();
            filterCriteria = conf.get("input");
             
            try {
                URI[] cacheFile = context.getCacheFiles();
                URI uri = cacheFile[0];
                BufferedReader bufferedReader = new BufferedReader(new FileReader(new File(uri.getPath()).getName()));
                String data = bufferedReader.readLine();
                while (data != null) {
                    if (data.contains(filterCriteria)) {
                        criteriaBusinessId.add(data.split("::")[0]);
                    }
                    data = bufferedReader.readLine();
                }
                bufferedReader.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
            String[] data = value.toString().split("::");
            String userId = data[1];
            String businessId = data[2];
            String rating = data[3];
              
            if (criteriaBusinessId.contains(businessId)) {
                context.write(new Text(userId.toString()), new Text(rating));
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Q4UserRatings <in1> <in2> <out>");
            System.exit(2);
        }
        conf.set("input", "Palo Alto,");
         
        Job job = Job.getInstance(conf, "findPaloAlto");
        job.setJarByClass(findPaloAlto.class);
 
        job.setMapperClass(MapFilter.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        Path input = new Path(otherArgs[0]);
        Path cacheFile = new Path(otherArgs[1]);
        Path output = new Path(otherArgs[2]);
 
        FileInputFormat.addInputPath(job, input);
 
        job.addCacheFile(cacheFile.toUri());
 
        FileSystem hdfsFS = FileSystem.get(conf);
        if (hdfsFS.exists(output)) {
            hdfsFS.delete(output, true);
        }
        FileOutputFormat.setOutputPath(job, output);
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
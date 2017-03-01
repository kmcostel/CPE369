// Kevin Costello and Holly Haraguchi
// CSC 369, Winter 2017
// Lab 8, Program 1: Accounting

// Section 1: Imports
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized Long wrapper class

// For Map and Reduce jobs
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function

// To start the MapReduce process
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver

// For File "I/O"
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename

// Hadoop job configuration and tools
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Exception handling
import java.io.IOException;

// JSON Simple
import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;

public class accounting extends Configured implements Tool {

    // Mapper Class
    // Input: (<key> , <ThghtShre JSON object>)
    // Outputs: (<user ID> , <message text>)
    public static class accountingMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException { 
            // Parse the value as a ThghtShre JSON object
            try {
                JSONParser parser = new JSONParser();
                Object obj = parser.parse(value.toString());

                JSONObject jsonObj = (JSONObject)obj;

                // Get attributes
                String user = (String)jsonObj.get("user");
                String msg = (String)jsonObj.get("text");

                // Output
                context.write(new Text(user), new Text(msg));
            }
            catch (Exception e) {
                System.out.println("ERROR: " + e.toString());
                System.exit(1);
            }
        }
    }

    // Reducer Class
    // Input: (<user ID> , <message text>)
    // Outputs: (<user ID> , <costs incurred>)
    public static class accountingReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalCents = 0;         
            int numMessages = 0;

            // Calculate messaging costs for each user
            for (Text val : values) {
                String msg = val.toString();
                int length = msg.length();

                // Each message incurs a charge of 5 cents
                totalCents += 5;

                // Every 10 bytes costs 1 cent
                totalCents += Math.ceil(length / 10);

                // If a message is longer than 100 bytes, 5 cent surcharge
                if (length > 100) {
                    totalCents += 5;
                }                             

                numMessages ++;
            }

            // Users who write more than 100 messages get an overall 5% discount
            if (numMessages > 100) {
                totalCents *= 0.05;
            }

            context.write(key, new Text(String.format("%.2f", totalCents / 100)));
        }
    }

    // Section 4: MapReduce Driver
    public int run(String[] args) throws Exception {
        // Step 0: Configuration
        Configuration conf = super.getConf();

        // Step 1: Get a new MapReduce Job object
        Job job = Job.getInstance(conf, "accounting");  

        // Step 2: Register the MapReduce class
        job.setJarByClass(accounting.class);  

        // Step 3: Set I/O files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4: Register mapper and reducer
        job.setMapperClass(accountingMapper.class);
        job.setReducerClass(accountingReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Prog 1 - Holly Haraguchi + Kevin Costello");
        
        // Step 7
        return job.waitForCompletion(true) ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new accounting(), args);
        System.exit(res);
    }
}



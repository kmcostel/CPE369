// Holly Haraguchi
// CSC 369, Winter 2017
// Lab 7, Program 1: Filtering
// Outputs < word with double letters , letter that is doubled >

// Section 1: Imports
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object

// Exception handling
import java.io.IOException;

public class repeatLetters {

    // Mapper Class
    public static class repeatMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Only want to report a single record of double letters
            // In this case, will be reporting the first instance of double letters if many exist
            boolean repeatFound = false; 

            // Make the job case-insensitve
            String word = value.toString().toLowerCase();

            if (!repeatFound) {
                // Compare adjacent characters to determine if any are repeated
                for (int i = 0; i < word.length() - 1; i++) {
                    char cur = word.charAt(i);
                    char next = word.charAt(i+1);

                    if (cur == next) {
                        context.write(value, new Text(String.valueOf(cur)));
                    }
                }
            }
        }
    }

    // Reducer Class
    public static class repeatReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, values.iterator().next());
        }
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(repeatLetters.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(repeatMapper.class);
        job.setReducerClass(repeatReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Repeat Letters - Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}



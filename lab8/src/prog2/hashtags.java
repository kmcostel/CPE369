// Kevin Costello and Holly Haraguchi
// CSC 369, Winter 2017
// Lab 8, Program 2: Hashtagging

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

// Import json-simple jar
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

// Exception handling
import java.io.IOException;

public class hashtags {

    // Mapper Class
    public static class repeatMapper extends Mapper< LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
/* General idea: extract message out of the value. Decompose the message to be in an arraylist of words. 
    For every word, output <userID, word> */             
        try {    
            // Create a parser
            JSONParser parser = new JSONParser();
            // Get the JSON string from input and  create a json object
            Object obj = parser.parse(value.toString());
            // Cast to a JSONObject
            JSONObject jsonObj = (JSONObject) obj;
            // Get the value of the user's message
            String message = (String) jsonObj.get("text");
            // Split up the messages into individual words, splitting by whitespace and commas.
            String[] words = message.split(" ");
            
            for (String s: words) {
                context.write(new Text(key.toString()), new Text(s));
            }
           // context.write(new Text(key.toString()), value);            
             
        } catch (Exception e) {
            // handle error here
        }

        }
    }

    // Reducer Class
    public static class repeatReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /* reduce gets a userID as key. |values| is basically a list of every word the user has ever written in their messages */
            
            for (Text val: values) {
                context.write(key, new Text(val.toString()));
            }
        }
    }

    // Section 4:  MapReduce Driver
    public static void main(String[] args) throws Exception {
        // Step 1: get a new MapReduce Job object
        Job job = Job.getInstance();  

        // Step 2: register the MapReduce class
        job.setJarByClass(hashtags.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 


/* We need to decide how to structure our input files on hdfs 
        KeyValueTextInputFormat.addInputPath(job, new Path("lab7_InputFiles/", "prog1-2.txt")); // put what you need as input file
        FileOutputFormat.setOutputPath(job, new Path("./lab7-pt1", "filter")); // put what you need as output file
        job.setInputFormatClass(KeyValueTextInputFormat.class);            // let's make input a CSV file
*/


        // Step 4:  Register mapper and reducer
        job.setMapperClass(repeatMapper.class);
        job.setReducerClass(repeatReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Lab 8 Program 2 - Kevin Costello, Holly Haraguchi");

        // Step 7
        System.exit(job.waitForCompletion(true) ? 0:1);
    }
}



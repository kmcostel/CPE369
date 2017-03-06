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
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Import json-simple jar
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.HashMap;

// Exception handling
import java.io.IOException;

public class hashtags extends Configured implements Tool {
    private static final String[] badWords = {"a", "the", "in", "on", "I", "he", "she", "it", "there", "is"};

    // Mapper Class
    public static class HashMapper extends Mapper< LongWritable, Text, Text, Text > {
         
        public boolean contains(String w) {
            for (String s: badWords) {
                if (s.equals(w)) return true;
            }
            return false;
        }

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
            String userId = (String) jsonObj.get("user");
            // Split up the messages into individual words, splitting by whitespace and commas.
            String[] words = message.split("[\\p{Punct}\\s]+");
            
            for (String s: words) {
                if (contains(s) == false)
                    context.write(new Text(userId), new Text(s.trim()));
            }
        } catch (Exception e) {
            // handle error here
        }

        }
    }

    // Reducer Class
    public static class HashReducer extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /* reduce gets a userID as key. |values| is basically a list of every word the user has ever written in their messages */

            // map with have words the user said be the key, and value will be a count
            HashMap<String, Integer> map = new HashMap<String, Integer>();
            
            // Track the max frequency of any word as reading in input
            int curMaxCount = 1;
            int count = 0;            
            
            for (Text val: values) {
                String s = val.toString();
                
                if (map.containsKey(s)) {
                    count = map.get(s) + 1;
                    map.put(s, count);
                    if (count > curMaxCount) 
                        curMaxCount = count;
                }
                else map.put(s, 1);
            }
            String frequentList = "";
            boolean first = true;
            // Iterate through the keys and output keys if their corresponding value is = to curMaxCount
            for (String mapKey: map.keySet()) {
                if (map.get(mapKey) == curMaxCount) {
                    if (first) {
                        frequentList += mapKey;
                        first = false;
                    }
                    else frequentList += ", " + mapKey;
                }
            }
            context.write(new Text(key.toString()), new Text(frequentList));

        }
    }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new hashtags(), args);
    System.exit(res);
  }
    @Override
    public int run(String args[]) throws Exception {
        Configuration conf = super.getConf();
        // Step 1: get a new MapReduce Job object
        Job job = new Job(conf, "Hashtag Job");

        // Step 2: register the MapReduce class
        job.setJarByClass(hashtags.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(HashMapper.class);
        job.setReducerClass(HashReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Lab 8 Program 2 - Kevin Costello, Holly Haraguchi");

        // Step 7
        return job.waitForCompletion(true) ? 0:1;
    }
}



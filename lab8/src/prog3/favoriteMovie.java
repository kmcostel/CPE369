// Kevin Costello and Holly Haraguchi
// CSC 369, Winter 2017
// Lab 8, Program 3: Favorite Movie by State

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


// General Game Plan!!!!!
/* Need to output favorite movie by state. This will require 3 mappers and 2 reducers.
 * Mapper1 and Mapper2 will both go to Reducer1
 * Mapper1 will emit <MovieID> as Key and <State, Rating> as Value
 * Mapper2 will emit <MovieID> as Key and <MovieTitle> as Value
 * Reducer1 will figure out the Movie's title by splitting the value on ",". The value with
 *  the movie's title will return a string[] of length=1, while all other values will have
 *  a length=2 (State, Rating).
 * Reducer1 will emit <State, MovieTitle> as Key and <AvgRating> as Value
 
 * Mapper2 will emit <State> as Key and <AvgRating, MovieTitle> as value.
 * Reducer2 will emit <State> as Key and <MovieTitle> as value.
 */


public class favoriteMovie extends Configured implements Tool {

    // Mapper1
    // Outputs: Key=<State, Movie> Value=<Rating>
    public static class Mapper1 extends Mapper< LongWritable, Text, Text, Text > {
         
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {    
            // Create a parser
            JSONParser parser = new JSONParser();
            // Get the JSON string from input and  create a json object
            Object obj = parser.parse(value.toString());
            // Cast to a JSONObject
            JSONObject jsonObj = (JSONObject) obj;
            // Get the value of the user's state and their list of ratings
            String state = (String) jsonObj.get("respondent.state");
            Double[] ratings = (Double[]) jsonObj.get("ratings");
    
        } catch (Exception e) {
            // handle error here
        }

        }
    }

    // Reducer1
    // Outputs: Key=<State, MovieTitle>, Value=<AvgRating>
    public static class Reducer1 extends Reducer< Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

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
        job.setJarByClass(favoriteMovie.class);  

        // Step 3:  Set Input and Output files
        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
        job.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job.setMapperClass(FavoriteMapper.class);
        job.setReducerClass(FavoriteReducer.class);

        // Step 5: Set up output information
        job.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job.setJobName("Lab 8 Program 2 - Kevin Costello, Holly Haraguchi");

        // Step 7
        return job.waitForCompletion(true) ? 0:1;
    }
}



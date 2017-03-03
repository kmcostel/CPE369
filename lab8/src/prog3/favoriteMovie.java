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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for pointing as multiple input files

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Import json-simple jar
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.HashMap;
import java.util.ArrayList;

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
 * Reducer1 will emit <State, MovieTitle> as Key and <Rating> as Value
 
 * Mapper3 can compute average rating; this will emit <State> as Key and <AvgRating, MovieTitle> as value.
 * Reducer2 will emit <State> as Key and <MovieTitle> as value.
 */


public class favoriteMovie extends Configured implements Tool {

    // Mapper1
    // Outputs: Key=<MovieID> Value=<State,Rating>
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
                String state = "CA";//(String) jsonObj.get("respondent.state");
                Double[] ratings = (Double[]) jsonObj.get("ratings");
                
                // Output each rating
                for (int i = 0; i < ratings.length; i++) {
                    context.write(new Text(Integer.toString(i+1)), new Text(state + "," + Double.toString(ratings[i])));
                }    
            } 
            catch (Exception e) {
                // Handle error here
                System.out.println("ERROR:" + e.toString());
                System.exit(1);
            }
        }
    }
    
    // Mapper 2
    // Outputs: Key=<MovieID>, Value=<MovieTitle>
    public static class Mapper2 extends Mapper < LongWritable, Text, Text, Text > {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String val = value.toString();
            String[] split = val.split(",");
            context.write(new Text(split[0]), new Text(split[1]));  
        }
    }

    // Reducer1
    // Matches movieID's with movie titles
    // Outputs: Key=<State, MovieTitle>, Value=<Rating>
    public static class Reducer1 extends Reducer< Text, Text, Text, Text > {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Filler Title";
            ArrayList<String> states = new ArrayList<String>();
            ArrayList<Double> ratings = new ArrayList<Double>();

            // Iterate over the values
            for (Text val : values) {
                String[] splits = val.toString().split(",");
                
                // We have a rating
                if (splits.length == 2) {
                    states.add(splits[0]);
                    ratings.add(Double.parseDouble(splits[1]));
                }
                // We have the movie title
                else {
                   movieTitle = splits[0];
                }
            }
            
            // Output
            for (int i = 0; i < states.size(); i++) {
                context.write(new Text(states.get(i) + "," + movieTitle), new Text(Double.toString(ratings.get(i))));
            }
        }
    }
    
    // Mapper 3
    // Reducer1 will emit <State, MovieTitle> as Key and <Rating> as Value
    // This just passes along values from Reducer1 to Reducer2
    /*public static class Mapper3 extends Mapper < Text, Text, Text, Text > {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {         
            context.write(key, value);
        }
    }
    
    
    // Reducer2
    // Calculates the average ratings for each movie by state
    // Outputs: Key=<State, MovieTitle>, Value=<AvgRating>
    // Mapper3 will emit <State, MovieTitle> as Key and <Rating> as value.
    public static class Reducer2 extends Reducer< Text, Text, Text, Text > {
        @Override
        public void reduce(Text key, Iterable<Text> ratings, Context context) throws IOException, InterruptedException {
            double totalRating = 0;
            int numRatings = 0;            
            
            // Iterate over the ratings to calculate the average rating by movie and state
            for (Text rating: ratings) {
                totalRating += Double.parseDouble(rating.toString());
                numRatings++;
            }
        
            context.write(key, new Text(Double.toString(totalRating / numRatings)));
        }
    }
    
    // Mapper 4
    // Input: Key=<State, MovieTitle>, Value=<AvgRating>
    // Output: Key=<State> and Value=<MovieTitle, AvgRating>
    public static class Mapper4 extends Mapper < Text, Text, Text, Text > {
        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {         
            String[] stateAndMovie = key.toString().split(",");
            String state = stateAndMovie[0].trim();
            String movieTitle = stateAndMovie[1].trim();
            
            String movieAndRating = movieTitle + "," + value.toString();
            
            context.write(new Text(state), new Text(movieAndRating));
        }
    }
    
    // Needs to find the max avgRating per state and movietitle
    // Input: Key=<State>, Value=<MovieTitle, AvgRating>
    public static class Reducer3 extends Reducer< Text, Text, Text, Text > {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String state = key.toString();
            Double maxAvgRating = new Double(0.0);
            String favMovie = "";           
            
            for (Text val: values) {
                String[] split = val.toString().split(",");
                String curMovieTitle = split[0];
                Double curRating = new Double(Double.parseDouble(split[1].toString()));
                
                if (curRating.compareTo(maxAvgRating) > 0) {
                    maxAvgRating = curRating;
                    favMovie = curMovieTitle;
                }
            }
        
            context.write(key, new Text(favMovie));
        }
    }*/

    
    // args = 
    //   json file,
    //   movies.csv,
    //   output directory
    @Override
    public int run(String args[]) throws Exception {
      // MapReduce Job #1
        Configuration conf = super.getConf();
        
        
        // Step 1: get a new MapReduce Job object
        Job job1 = Job.getInstance(conf);

        // Step 2: register the MapReduce class
        job1.setJarByClass(favoriteMovie.class);  

        // Step 3:  Set Input and Output files
        // have input file for movies.csv and for movie rating json
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Mapper1.class);      
//        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Mapper2.class);
        
        FileOutputFormat.setOutputPath(job1, new Path(args[2])); 
        //job1.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job1.setReducerClass(Reducer1.class);

        // Step 5: Set up output information
        job1.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job1.setJobName("Lab 8 Program 3, Job #1 - kmcostel, hharaguc");

        // Step 7
//        job1.waitForCompletion(true);

    /* // MapReduce Job #2
        // Step 1: get a new MapReduce Job object
        Job job2 = Job.getInstance();

        // Step 2: register the MapReduce class
        job2.setJarByClass(favoriteMovie.class);  

        // Step 3:  Set Input and Output files
        // Set input to be the output part-r-00000 file from job #1
        FileInputFormat.addInputPath(job2, new Path("./test/job1Out/part-r-00000")); 
        FileOutputFormat.setOutputPath(job2, new Path("./test/job2Out")); 
        
        job2.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job2.setMapperClass(Mapper3.class);
        job2.setReducerClass(Reducer2.class);

        // Step 5: Set up output information
        job2.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job2.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job2.setJobName("Lab 8 Program 3, Job #2 - kmcostel, hharaguc");

        // Step 7
        job2.waitForCompletion(true);
        
        // MapReduce Job #3
        // Step 1: get a new MapReduce Job object
        Job job3 = Job.getInstance();

        // Step 2: register the MapReduce class
        job3.setJarByClass(favoriteMovie.class);  

        // Step 3:  Set Input and Output files
        // Set input to be the output part-r-00000 file from job #1
        FileInputFormat.addInputPath(job3, new Path("./test/job2Out/part-r-00000")); 
        FileOutputFormat.setOutputPath(job3, new Path(args[2])); 
        
        job3.setInputFormatClass(TextInputFormat.class); 

        // Step 4:  Register mapper and reducer
        job3.setMapperClass(Mapper4.class);
        job3.setReducerClass(Reducer3.class);

        // Step 5: Set up output information
        job3.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job3.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value

        // Step 6: Set up other job parameters at will
        job3.setJobName("Lab 8 Program 3, Job #3 - kmcostel, hharaguc");
*/
        // Step 7
        return job1.waitForCompletion(true) ? 0 : 0;
                 
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new favoriteMovie(), args);
        System.exit(res);
    }
}

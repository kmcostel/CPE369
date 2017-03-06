// Kevin Costello and Holly Haraguchi

// CSC 369: Distributed Computing
// Alex Dekhtyar
// Multiple input files

// Section 1: Imports
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // key-value input files
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.json.simple.parser.JSONParser;
import org.json.simple.JSONObject;
import org.json.simple.JSONArray;
import org.json.simple.parser.ParseException;

public class favoriteMovie extends Configured implements Tool {

    /*
     * Job 1: Join Movie IDs and Movie Titles
     */
     
    // Mapper for movies.csv file
    public static class MoviesCsvMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(",");
            if (splits.length == 2) {
                context.write(new Text(splits[0]), new Text(splits[1]));
            }            
        } 
    }
    
    // Mapper for input JSON file
    public static class InputJsonMapper extends Mapper< LongWritable, Text, Text, Text > {              
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                ArrayList<Double> ratings = new ArrayList<Double>();

                // Create a parser
                JSONParser parser = new JSONParser();
                
                // Get the JSON string from input and  create a json object
                Object obj = parser.parse(value.toString());
                
                // Cast to a JSONObject
                JSONObject jsonObj = (JSONObject) obj;
                
                // Get the value of the user's state and their list of ratings
                JSONObject resp = (JSONObject) jsonObj.get("respondent");
                String state = (String) resp.get("state");
                
                JSONArray ratingsArr = (JSONArray) jsonObj.get("ratings");
                Iterator<Double> iter = ratingsArr.iterator();
                while (iter.hasNext()) {
                    ratings.add(iter.next());
                }
                
                for (int i = 0; i < ratings.size(); i++) {
                    context.write(new Text(Integer.toString(i+1)), new Text(state + "," + ratings.get(i)));
                }    
            }
            catch (Exception e) {
               System.out.println("ERROR: " + e.toString());
            }
        }
    }
    
    // Joins Movie IDs and Movie Titles
    // Inputs: <Movie ID>, <State, Movie Rating> and <Movie ID>, <Movie Title>
    // Outputs: <Movie Title, State>, <Respective Movie Rating>   
    public static class JoinReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> states = new ArrayList<String>();
            ArrayList<String> ratings = new ArrayList<String>();
            String movieTitle = "";
            
            for (Text val : values) {
                String[] splits = val.toString().split(",");
                
                // Dealing with a rating; add the rating to the ArrayList for later access
                if (splits.length == 2) {
                    states.add(splits[0]);
                    ratings.add(splits[1]);
                }
                else {
                    movieTitle = splits[0];
                }
            }
            
            // Output the movie ratings with their respective movie title and state
            for (int i = 0; i < states.size(); i++) {
                context.write(new Text(movieTitle + "," + states.get(i)), new Text(ratings.get(i)));
            }            
        }        
    }
    
    // Input: JoinReducer's output; <Movie Title, State>, <Respective Movie Rating>
    public static class AverageMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            context.write(new Text(splits[0]), new Text(splits[1]));           
        } 
    }
    
    // Input: <Movie Title, State>, <Respective Movie Ratings> 
    // Output: <Movie Title, State>, <Average Movie Rating>
    // Calculates the average rating of |Movie Title| for |State|
    // Excludes ratings of value '0'
    public static class AverageReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double zero = new Double(0.0);
            double totalRating = 0.0;
            int numUsers = 0;
            
            // Iterate over ratings
            for (Text val : values) {
                Double rating = Double.parseDouble(val.toString());
                
                // Dealing with a non-zero rating
                if (rating.compareTo(zero) > 0) {
                    totalRating += rating.doubleValue();
                    numUsers++;
                }               
            }
            
            context.write(key, new Text(Double.toString(totalRating / numUsers)));
        }
    }
    
    // Input: AverageReducer's output; <Movie Title, State>, <Average Movie Rating>
    // Output: <State>, <Movie Title, Average Movie Rating>
    public static class MaxMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String origKey = splits[0];
            String movieTitle = origKey.split(",")[0];
            String state = origKey.split(",")[1];
            
            // splits[1] = avgMovieRating
            context.write(new Text(state), new Text(movieTitle + "," + splits[1]));           
        } 
    }
    
    // Input: <State>, <Movie Title, Average Movie Rating>
    // Output: <State>, <Movie Title>
    // Determines which movie has the highest average rating for each state
    public static class MaxReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double maxRating = new Double(0.0);
            String maxTitle = "";
            
            // Iterate over the state's average movie ratings
            for (Text val : values) {
                String[] splits = val.toString().split(",");
                
                Double curRating = Double.parseDouble(splits[1]);
                
                // New max rating found
                if (curRating.compareTo(maxRating) > 0) {
                    maxRating = curRating;
                    maxTitle = splits[0];
                }
            }
            
            context.write(key, new Text(maxTitle));
        }
    }
    
    //  MapReduce Driver       
    public int run(String[] args) throws Exception {
        
        Configuration conf = super.getConf();
        
        // Job 1: Join Movie IDs and Movie Titles
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(favoriteMovie.class);
        
        //  Set up multiple inputs
        MultipleInputs.addInputPath(job1, new Path(args[0]),
        TextInputFormat.class, MoviesCsvMapper.class );
        MultipleInputs.addInputPath(job1, new Path(args[1]),
        TextInputFormat.class, InputJsonMapper.class );
        
        FileOutputFormat.setOutputPath(job1, new Path("./test/","join-out")); // put what you need as output file
        
        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        // step 6: Set up other job parameters at will
        job1.setJobName("Join Movie IDs and Movie Titles");
        
        job1.waitForCompletion(true);
        
        // Job 2: Calculate average movie rating by Movie Title and Stat
        
        // Step 1: get a new MapReduce Job object
        Job job2 = Job.getInstance();
        
        // Step 2: register the MapReduce class
        job2.setJarByClass(favoriteMovie.class); 
         
        // Step 3:  Set Input and Output files
        // Set input to be the output part-r-00000 file from Job #1
        FileInputFormat.addInputPath(job2, new Path("./test/join-out/part-r-00000")); 
        FileOutputFormat.setOutputPath(job2, new Path("./test/average-out"));         
       // job2.setInputFormatClass(TextInputFormat.class); 
        
        // Step 4:  Register mapper and reducer
        job2.setMapperClass(AverageMapper.class);
        job2.setReducerClass(AverageReducer.class);
        
        // Step 5: Set up output information
        job2.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job2.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value
        
        // Step 6: Set up other job parameters at will
        job2.setJobName("Calculate Avg");
        
        // Step 7
        job2.waitForCompletion(true);
        
        
        // Job 3: Find the movie with the highest average rating, for each state
        Job job3 = Job.getInstance();
        
        // Step 2: register the MapReduce class
        job3.setJarByClass(favoriteMovie.class); 
         
        // Step 3:  Set Input and Output files
        // Set input to be the output part-r-00000 file from Job #1
        FileInputFormat.addInputPath(job3, new Path("./test/average-out/part-r-00000")); 
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));         
       // job2.setInputFormatClass(TextInputFormat.class); 
        
        // Step 4:  Register mapper and reducer
        job3.setMapperClass(MaxMapper.class);
        job3.setReducerClass(MaxReducer.class);
        
        // Step 5: Set up output information
        job3.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job3.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value
        
        // Step 6: Set up other job parameters at will
        job3.setJobName("Max Rating ");
        
        // Step 7
        return job3.waitForCompletion(true) ? 0 : 1;
     
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        int res = ToolRunner.run(conf, new favoriteMovie(), args);
        System.exit(res);
    }
    
}

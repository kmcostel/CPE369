// Kevin Costello and Holly Haraguchi
// CSC 369, Winter 2017
// Lab 8, Prog 4 - User Similarity

// Section 1: Imports
import java.util.ArrayList;
import java.util.Iterator;

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

public class userSim extends Configured implements Tool {
    // Mapper for the input JSON File
    // Outputs each JSON object with the same key
    public static class InputJsonMapper extends Mapper< LongWritable, Text, Text, Text > {  
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("key"), value);          
        } 
    }
       
    // Input: <Key>, <MovieRatings JSON Object>
    // Output: <User1, User2>, <Similarity>
    public static class SimReducer extends Reducer< Text, Text, Text, Text> {  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
              // Initialize a JSON parser
              JSONParser parser = new JSONParser();                            
              
              ArrayList<JSONObject> users = new ArrayList<JSONObject>();
              for (Text val: values) {
                  Object obj = parser.parse(val.toString());
                  JSONObject userObj = (JSONObject) obj;
                  users.add(userObj);
              }
                  
              // Output <key, value> pairs for each pair of users
              for (int i = 0; i < users.size(); i++) {
                  for (int j = i + 1; j < users.size(); j++) {
                      JSONObject u1 = users.get(i);
                      JSONObject u2 = users.get(j);
                      
                      JSONObject u1NameObj = (JSONObject) ((JSONObject)u1.get("respondent")).get("name");
                      JSONObject u2NameObj = (JSONObject) ((JSONObject)u2.get("respondent")).get("name");
                      
                      String u1Name = ((String) u1NameObj.get("first")) + " " + ((String) u1NameObj.get("last"));
                      String u2Name = ((String) u2NameObj.get("first")) + " " + ((String) u2NameObj.get("last"));
                      String newKey = u1Name + ", " + u2Name;
                      
                      JSONArray ratingsArr1 = (JSONArray) u1.get("ratings");
                      JSONArray ratingsArr2 = (JSONArray) u2.get("ratings");
                  
                      ArrayList<Double> u1Ratings = new ArrayList<Double>();
                      ArrayList<Double> u2Ratings = new ArrayList<Double>();
                  
                      // Save the users' ratings arrays into ArrayLists
                      Iterator<Double> iter1 = ratingsArr1.iterator();
                      while (iter1.hasNext()) {
                          u1Ratings.add(iter1.next());   
                      }  
                  
                      Iterator<Double> iter2 = ratingsArr2.iterator();
                      while (iter2.hasNext()) {
                          u2Ratings.add(iter2.next());   
                      } 
                      
                      double sim = calcSim(u1Ratings, u2Ratings);
                      context.write(new Text(newKey), new Text(Double.toString(sim)));
                  }
              }
            }
            catch (Exception e) {
                System.out.println("ERROR: " + e.toString());
                System.exit(1);
            }                      
        }       
              
        // Calculates the Pearson Correlation similarity between two users
        private static double calcSim(ArrayList<Double> u1Ratings, ArrayList<Double> u2Ratings) {
            double u1Avg = getAvg(u1Ratings);
            double u2Avg = getAvg(u2Ratings);
       
            double numerator = 0.0;      
            double xDenomSum = 0.0;
            double yDenomSum = 0.0;
            double denominator = 0.0;

            for (int i = 0; i < u1Ratings.size(); i++) {
              numerator += (u1Ratings.get(i) - u1Avg) * (u2Ratings.get(i) - u2Avg);
              
              xDenomSum += ((u1Ratings.get(i) - u1Avg) * (u1Ratings.get(i) - u1Avg));
              yDenomSum += ((u2Ratings.get(i) - u2Avg) * (u2Ratings.get(i) - u2Avg));
            }

            denominator = Math.sqrt(xDenomSum) * Math.sqrt(yDenomSum);

            return Math.abs(numerator / denominator);
        }
   
        /* Calculates a user's non-zero ratings average */
        private static double getAvg(ArrayList<Double> ratings) {
            int nonZeroRatings = 0;
            double sumRatings = 0.0;

            for (int i = 0; i < ratings.size(); i++) {
               if (ratings.get(i) > 0.0) {
                   sumRatings += ratings.get(i);
                   nonZeroRatings++;
               }
            }

            /* Compute average of movie's the user has seen (average of non-zero ratings) */   
            return sumRatings / nonZeroRatings;   
        }
    }
    
    //  MapReduce Driver       
    public int run(String[] args) throws Exception {       
        Configuration conf = super.getConf();
               
        // Initialize the job
        Job job = Job.getInstance(conf);
        job.setJarByClass(userSim.class);
        
        // Set I/O
        FileInputFormat.setInputPaths(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 
                
        // Set the map and reduce classes
        job.setMapperClass(InputJsonMapper.class);
        job.setReducerClass(SimReducer.class);
        
        // Specify the output format classes
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        job.setJobName("Find User Similarity");
        
        return job.waitForCompletion(true) ? 0 : 1;    
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();       
        int res = ToolRunner.run(conf, new userSim(), args);
        
        System.exit(res);
    }
    
}

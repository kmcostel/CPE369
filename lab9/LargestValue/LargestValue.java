// Holly Haraguchi and Kevin Costello
// CSC 369, Winter 2017
// Lab 9
// Computes the number of hands with the highest value in each file

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
import java.util.ArrayList;

public class LargestValue {
    // Mapper for poker-hand-testing file - file #1
    public static class HandValueMapper1 extends Mapper< LongWritable, Text, Text, Text > { 
        private int maxVal = 0;
        private int numMax = 0;
               
        @Override       
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            String[] vals = value.toString().split(",");
            
            // Iterate over the cards to find the value of the hand
            for (int i = 1; i < vals.length; i+=2) {
                int cardVal = Integer.parseInt(vals[i]);
                // Encountered an ace
                if (cardVal == 1) {
                   sum += 14;
                }
                else {
                   sum += cardVal;
                }
            }
            
            // New highest value found
            if (sum > maxVal) {
                maxVal = sum;
                numMax = 1;
            }
            else if (sum == maxVal) {
                numMax++;
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("1"), new Text(maxVal + "," + numMax));
        } 
    }
    
    // Mapper for poker-hand-training file - file #2
    public static class HandValueMapper2 extends Mapper< LongWritable, Text, Text, Text > {     
        private int maxVal = 0;
        private int numMax = 0;   
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            String[] vals = value.toString().split(",");
            
            // Iterate over the cards to find the value of the hand
            for (int i = 1; i < vals.length; i+=2) {
                int cardVal = Integer.parseInt(vals[i]);
                // Encountered an ace
                if (cardVal == 1) {
                   sum += 14;
                }
                else {
                   sum += cardVal;
                } 
            }
            
            // New highest value found
            if (sum > maxVal) {
                maxVal = sum;
                numMax = 1;
            }
            else if (sum == maxVal) {
                numMax++;
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("2"), new Text(maxVal + "," + numMax));
        }  
    }
    
    // Inputs: 
    // Outputs: 
    public static class MaxHandReducer extends Reducer< Text, Text, Text, Text> {   
        private int maxHand1 = 0;
        private int numMaxHand1 = 0;
        
        private int maxHand2 = 0;
        private int numMaxHand2 = 0;
                   
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String fileNum = key.toString();
            
            for (Text val : values) {
                String[] splits = val.toString().split(",");
                int curVal = Integer.parseInt(splits[0]);
                int curCnt = Integer.parseInt(splits[1]);
                
                // Dealing with File #1 values; compare to File #1 variables
                if (fileNum.equals("1")) {
                    if (curVal > maxHand1) {
                        maxHand1 = curVal;
                        numMaxHand1 = curCnt;
                    }
                }
                // Dealing with File #2 values; compare to File #2 variables
                else {
                    if (curVal > maxHand2) {
                        maxHand2 = curVal;
                        numMaxHand2 = curCnt;
                    }
                }
            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (numMaxHand1 > numMaxHand2) {
                context.write(new Text("Highest value: " + maxHand1), new Text("poker-hand-testing.data.txt"));
            }
            else if (numMaxHand2 > numMaxHand1) {
                context.write(new Text("Highest value: " + maxHand2), new Text("poker-hand-traning.true.data.txt"));
            }
            else {
                context.write(new Text("Same number of hands with the highest value"), new Text("poker-hand-testing.data.txt and poker-hand-traning.true.data.txt"));
            }
        }                  
    }
     
    
    //  MapReduce Driver       
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        // Job 1: Join Movie IDs and Movie Titles
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(LargestValue.class);
        
        // Set up multiple inputs
        MultipleInputs.addInputPath(job1, new Path("/data/","poker-hand-testing.data.txt"),
        TextInputFormat.class, HandValueMapper1.class );
        MultipleInputs.addInputPath(job1, new Path("/data/","poker-hand-traning.true.data.txt"),
        TextInputFormat.class, HandValueMapper2.class );
        
        FileOutputFormat.setOutputPath(job1, new Path("LargestValue-output")); // put what you need as output
        
        // Set up the reducer                
        job1.setReducerClass(MaxHandReducer.class);
        job1.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        // step 6: Set up other job parameters at will
        job1.setJobName("Largest Poker Hand Value");
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);   
    
   }    
}

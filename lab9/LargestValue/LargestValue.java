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
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            String[] vals = value.toString().split(",");
            
            // Iterate over the cards to find the value of the hand
            for (int i = 1; i < vals.length; i+=2) {
                sum += Integer.parseInt(vals[i]);
            }
            context.write(new Text("key"), new Text(sum + "," + "1"));          
        } 
    }
    
    // Mapper for poker-hand-training file - file #2
    public static class HandValueMapper2 extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            int sum = 0;
            String[] vals = value.toString().split(",");
            
            // Iterate over the cards to find the value of the hand
            for (int i = 1; i < vals.length; i+=2) {
                sum += Integer.parseInt(vals[i]);
            }
            context.write(new Text("key"), new Text(sum + "," + "2"));          
        } 
    }
    
    // Inputs: 
    // Outputs: 
    public static class MaxHandReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int maxSumF1 = 0;
            int maxSumF2 = 0;
            int freqf1 = 0;
            int freqf2 = 0;
            
            ArrayList<Integer> f1Vals = new ArrayList<Integer>();
            ArrayList<Integer> f2Vals = new ArrayList<Integer>();
            
            // Find the max value of each file
            for (Text val: values) {
                // str[0] = sum; str[1] = origFile
                String[] str = val.toString().split(",");
                int sum = Integer.parseInt(str[0]);
                int fileNum = Integer.parseInt(str[1]);
                
                if (fileNum == 1) {
                    // Compare to File 1's max hand
                    if (sum > maxSumF1) {
                        maxSumF1 = sum;
                    }                 
                    // Add the value to the ArrayList for future iteration
                    f1Vals.add(sum);
                }
                else {
                    // Compare to File 2's max hand
                    if (sum > maxSumF2) {
                        maxSumF2 = sum;
                    }
                    
                    // Add the value to the ArrayList for future iteration
                    f2Vals.add(sum);
                }
            }
            
            // Count the number of hands with the highest value in each file
            for (int i = 0; i < f1Vals.size(); i++) {
                if (f1Vals.get(i) == maxSumF1) {
                    freqf1++;
                }
            }
            
            for (int i = 0; i < f2Vals.size(); i++) {
                if (f2Vals.get(i) == maxSumF2) {
                    freqf2++;
                }
            }
            
            // Output the file name with a higher frequency of highest hands
            // Note: we don't care which file has the hands of highest value
            if (freqf1 > freqf2) {
                context.write(new Text("1"), new Text("poker-hand-testing.data.txt"));
            }
            else {
                context.write(new Text("1"), new Text("poker-hand-traning.true.data.txt"));
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
        
        FileOutputFormat.setOutputPath(job1, new Path("./LargestValue-output")); // put what you need as output
        
        // Set up the reducer                
        job1.setReducerClass(MaxHandReducer.class);
        job1.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        // step 6: Set up other job parameters at will
        job1.setJobName("Largest Poker Hand Value");
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);   
    
   }    
}

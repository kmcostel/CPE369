// Holly Haraguchi and Kevin Costello
// CSC 369, Winter 2017
// Lab 9, Problem 7: Power Days

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


public class PowerDays {

    /*
     * Job 1: Calculate power usage for individual days
     */
     
     // Outputs: <Date>, <Usage by minute>
     public static class SingleDayMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // TODO Get the date
            String text = value.toString();
            String[] splits = text.split(";");
            String usage = splits[3];
            
            // Dealing with valid input; NOT the first line of the file
            if (text.charAt(0) != 'D' || !(usage.equals("?"))) {                            
                String date = splits[0];
                
                // Output the date with its current usage
                context.write(new Text(date), new Text(usage));
            }     
        } 
    }
    
    // Calculates total power usage for a single day
    // Inputs: <Date>, <Usage by minute>
    // Outputs: <Date>, <Usage for entire day>   
    public static class SingleDayReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String date = key.toString();
            String year = date.split("/")[2];
            double totalUsage = 0.0;
            double curUse = 0.0;
            
            // Total up the usage for today's |date|
            for (Text val : values) {
                curUse = Double.parseDouble(val.toString());
                totalUsage += curUse;                
            }     
            
            context.write(new Text(year), new Text(date + ";" + curUse));     
        }        
    }
    
    /*
     * Job 2: Outputs each year's day of highest power usage
     */
     
    // Input: SingleDayReducer's output; <Date>, <Usage for entire day> 
    // Output: < Year, <Usage for entire day, Date> >
    public static class YearMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            context.write(new Text(splits[0]), new Text(splits[1]));           
        } 
    }
    
    // Finds the highest day of power usage in each year
    // Input: < Year, <Usage for entire day, Date> >
    // Output: < Year, <Usage for highest power day, Date of highest usage in year> >
    public static class YearReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double highestUsage = 0.0;
            String highestDate = "";
            
            // Find each year's highest day
            for (Text val : values) {
                String[] splits = val.toString().split(";");
                String date = splits[0];
                double curUsage = Double.parseDouble(splits[1]);
                
                // New highest usage day found
                if (Double.compare(curUsage, highestUsage) > 0) {
                    highestUsage = curUsage;
                    highestDate = date;
                }
            }
            
            context.write(new Text(highestDate), new Text(Double.toString(highestUsage)));
        }
    }
    
    //  MapReduce Driver       
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        /*
         * Job 1: Calculate power usage for entire days
         */
         
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(PowerDays.class);
        
        // Set up file I/O
        FileInputFormat.addInputPath(job1, new Path("/data/household_power_consumption.txt"));        
        FileOutputFormat.setOutputPath(job1, new Path("./power-out")); // put what you need as output file
        
        // Set Mapper and Reducer
        job1.setMapperClass(SingleDayMapper.class);
        job1.setReducerClass(SingleDayReducer.class);
        
        // Specify the output format
        job1.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        // step 6: Set up other job parameters at will
        job1.setJobName("Calculate total usage for single day");
        
        job1.waitForCompletion(true);
        
        /*
         * Job 2: Find the day of highest power usage for each year
         */
         
        // Step 1: get a new MapReduce Job object
        Job job2 = Job.getInstance();
        
        // Step 2: register the MapReduce class
        job2.setJarByClass(PowerDays.class); 
         
        // Step 3:  Set Input and Output files
        // Set input to be the output part-r-00000 file from Job #1
        FileInputFormat.addInputPath(job2, new Path("./power-out/part-r-00000")); 
        FileOutputFormat.setOutputPath(job2, new Path("./test/highest-out"));         
        
        // Step 4:  Register mapper and reducer
        job2.setMapperClass(YearMapper.class);
        job2.setReducerClass(YearReducer.class);
        
        // Step 5: Set up output information
        job2.setOutputKeyClass(Text.class); // Specify the output class (what reduce() emits) for key
        job2.setOutputValueClass(Text.class); // Specify the output class (what reduce() emits) for value
        
        // Step 6: Set up other job parameters at will
        job2.setJobName("Power Days");
        
        // Step 7
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
     
    }
}

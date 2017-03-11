// Holly Haraguchi and Kevin Costello
// CSC 369, Winter 2017
// Lab 9 - Problem 8 Power Consumption
// Finds the 20 largest differences in power consumption in the kitchen

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

public class KitchenUse {
    // Mapper for poker-hand-testing file - file #1
    public static class KitchenMapper extends Mapper< LongWritable, Text, Text, Text > {        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignore first line in input (First character is a "D")
            // Input Format
// Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3;
// 16/12/2006;17:24:00;4.216;0.418;234.840;18.4000;0.000;1.000;17.000
// sub_metering_1 is the kitchen!!!!!
            
            String text = value.toString();
            
            // Dealing with valid input; NOT the first line of the file
            if (text.charAt(0) != 'D') {
                String[] splits = text.split(",");
                String time = splits[0];
                String kitchUse = splits[6];
            }
            
            String[] hrMinSecSplit = time.split(":");
            int hour = Integer.parseInt(hrMinSecSplit[0]);
            int min = Integer.parseInt(hrMinSecSplit[1]);
            
            
            // TODO convert time5 to move over to next day
            String time0 = Integer.toString(hour) + ":" + Integer.toString(min) + ":00";
            String time5 = Integer.toString(hour) + ":" + Integer.toString(min + 5) + ":00";
            
            // Are we only interested in the sub_metering_1 reading and the time? ....
            // Key = <time>, Value = <OriginalTime,power>
            context.write(new Text(time0), new Text(time0 +","+kitchUse));
            context.write(new Text(time5), new Text(time0 +","+kitchUse));
                    
        } 
    }
    
    // Inputs: Key=<date,time>, Value=[ (<time1, <time1, powerVal1>), (<time1>, <time5, powerVal2>) ]
    // Outputs: 
    public static class KitchenReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            String time = key.toString();
            ArrayList<Double> powers = new ArrayList<Double>();
            // Order isn't guaranteed probably...
            // Will have to check times after to see which came first
            // Order matters, need to compare time1 to time5
            String val1 = "";
            String val5 = "";

            for (Text v: values) {
               if (time1.equals("")) {
                  val1 = v.toString();
               }
               else {
                  val2 = v.toString();
               }
            }
            
            // Every time can't pair with another time+5 (the last4 times?)
            if (!v2.equals("")) {
               
               String[] v1Split = val1.split(",");
               String time1 = v1Split[0];
               String power1 = v1Split[1];
               
               
               String[] v2Split = val2.split(",");
               String time5 = v2Split[0];
               String power5 = v2Split[1];
               
               
            }
            
            
        }        
    }
     
    
    //  MapReduce Driver       
    public static void main(String[] args) throws Exception {
        
        Configuration conf = new Configuration();
        
        // Job 1: Join Movie IDs and Movie Titles
        Job job1 = Job.getInstance(conf);
        job1.setJarByClass(KitchenUse.class); 
        
        
        // Set up inputs
        MultipleInputs.addInputPath(job1, new Path("/data", "/household_power_consumption.txt"),
        TextInputFormat.class, Kitchen.class );
        
        FileOutputFormat.setOutputPath(job1, new Path("kitchen-output")); // put what you need as output
        
        // Set up the reducer                
        job1.setReducerClass(KitchenReducer.class);
        job1.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job1.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
        // step 6: Set up other job parameters at will
        job1.setJobName("HouseHold Kitchen Power");
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);   
    
   }    
}

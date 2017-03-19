// Holly Haraguchi and Kevin Costello
// CSC 369, Winter 2017
// Lab 9, Problem 8 - Kitchen Use 
// Finds the 20 largest differences in power consumption in the kitchen

import org.apache.hadoop.io.IntWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.LongWritable; // Hadoop's serialized int wrapper class
import org.apache.hadoop.io.Text;        // Hadoop's serialized String wrapper class
import org.apache.hadoop.mapreduce.Mapper; // Mapper class to be extended by our Map function
import org.apache.hadoop.mapreduce.Reducer; // Reducer class to be extended by our Reduce function
import org.apache.hadoop.mapreduce.Job; // the MapReduce job class that is used a the driver
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // key-value input files
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename

import java.io.IOException;
import java.util.ArrayList;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Set;
import java.util.PriorityQueue;
import java.util.Comparator;

public class KitchenUse {
    public static class KitchenMapper extends Mapper< LongWritable, Text, Text, Text > {    
        // Have a linked list as queue to store the current 5 time values    
        private static Queue<String> lastFive;
        // Have a priority queue for Top-K usage differences
        private static PriorityQueue<String> topDiffs;
        private static int K = 20;
        public static Comparator<String> UsageDiffComparator = new Comparator<String>() {
            @Override
            public int compare(String x, String y) {
                double firstUsage = Double.parseDouble(x.split("///")[0]);
                double secUsage = Double.parseDouble(y.split("///")[0]);

                int compareVal = Double.compare(firstUsage, secUsage);
                if (compareVal < 0) {
                   return -1;
                }
                else if (compareVal > 0) {
                   return 1;
                }
                return 0;
            }
        };
   
        @Override
        // Initialize our priority queue
        protected void setup(Context context) throws IOException, InterruptedException { 
            lastFive = new LinkedList<String>();
            topDiffs = new PriorityQueue<String>(K, UsageDiffComparator);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignore first line in input (First character is a "D")
            // Input Format
            // Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3;
            // 16/12/2006;17:24:00;4.216;0.418;234.840;18.4000;0.000;1.000;17.000
            // sub_metering_1 is the kitchen!!!!!
            
            String text = value.toString();
            String[] splits = text.split(";");
            // Dealing with valid input; NOT the first line of the file and does not contain garbage usage readings
            if (text.charAt(0) != 'D' && !(splits[3].equals("?"))) {            
                String dateTimeStr = splits[0] + " " + splits[1];
 
                // Parse the kitchen usage
                double kitchUsage = Double.parseDouble(splits[6]);
                String curData = dateTimeStr + ";" + kitchUsage;
                
                if (lastFive.size() == 5) {                    
                    String firstData = lastFive.remove();
                    double firstUsage = Double.parseDouble(firstData.split(";")[1]);
                    double diff = kitchUsage - firstUsage;
                    String usagePair = diff + "///" + firstData + "///" + curData;
                    
                    if (topDiffs.size() < K) {
                       topDiffs.add(usagePair);
                    } 
                    else {
                       double oldDiff = Double.parseDouble(topDiffs.peek().split("///")[0]);
                       
                       // Larger usage difference found
                       if (Double.compare(diff, oldDiff) > 0) {
                           topDiffs.remove();
                           topDiffs.add(usagePair);
                       }
                    }
               }
                
               lastFive.add(curData);                             
           }
                           
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
           // Write the top 20 usage differences with key=<usageDifference> and value=<individualTimeUsageInfo>
           for (String obj : topDiffs) {
               String[] splits = obj.split("///");
               context.write(new Text(splits[0]), new Text(splits[1] + " " + splits[2]));
           }
        }
    }
    
    public static class KitchenReducer extends Reducer< Text, Text, Text, Text> {              
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t: values) {
                context.write(key, t);
            }
        }        
    }
     
    
    //  MapReduce Driver       
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        // Initialize the job
        Job job = Job.getInstance(conf);
        job.setJarByClass(KitchenUse.class);
        
        // Set I/O
        FileInputFormat.addInputPath(job, new Path("/data/household_power_consumption.txt"));
        FileOutputFormat.setOutputPath(job, new Path("kitchenUse-output")); // put what you need as output
                
        // Set the map and reduce classes
        job.setMapperClass(KitchenMapper.class);
        job.setReducerClass(KitchenReducer.class);
        
        // Specify the output format classes
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
         
        /*job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.addInputPath(job, new Path("/data/household_power_consumption.txt"));
        job.getConfiguration().setInt(
                "mapreduce.input.lineinputformat.linespermap", 800); 
        */
        
        job.setJobName("Household Kitchen Power");
                
        System.exit(job.waitForCompletion(true) ? 0 : 1);       
   }    
}

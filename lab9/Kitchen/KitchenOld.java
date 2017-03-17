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
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; // class for  standard text input
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs; // class for "pointing" at input file(s)
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // class for "pointing" at output file
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat; // key-value input files
import org.apache.hadoop.conf.Configuration; // Hadoop's configuration object
import org.apache.hadoop.fs.Path;                // Hadoop's implementation of directory path/filename

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Queue;
import java.util.LinkedList;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Set;

public class KitchenUse {
    
    // Mapper for poker-hand-testing file - file #1
    public static class KitchenMapper extends Mapper< LongWritable, Text, Text, Text > {    
        // Have a linked list as queue to store the current 5 time values    
        Queue<MyData> myQ = new LinkedList<MyData>();
        HashMap<Double, UsagePair> map = new HashMap<Double, UsagePair>();
        
        private class MyData {
           Date myDate;
           double power;
           public MyData(Date date, double p) {
              myDate = date;
              power = p;
           }
           public double getPower() {return power;}
           public Date getDate() {return myDate;}
        }
        
        private class UsagePair {
           MyData data1;
           MyData data2;
           public UsagePair (MyData d1, MyData d2) {
              data1 = d1;
              data2 = d2;
           }
           
           public String toString() {
               return data1.getDate().toString() + "," + data1.getPower() + "," + data2.getDate().toString() + "," + data2.getPower();
           }
        }
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Ignore first line in input (First character is a "D")
            // Input Format
            // Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3;
            // 16/12/2006;17:24:00;4.216;0.418;234.840;18.4000;0.000;1.000;17.000
            // sub_metering_1 is the kitchen!!!!!
            
            String text = value.toString();
            
            // Dealing with valid input; NOT the first line of the file
            if (text.charAt(0) != 'D' || text.charAt(0) != '?') {            
                String[] splits = text.split(";");
                Date date = new Date();
                
                // Set the current date and time
                SimpleDateFormat sdf = new SimpleDateFormat("dd/mm/yyyy HH:mm:ss");
                try {
                    String dateTimeStr = splits[0] + " " + splits[1];
                    date = sdf.parse(dateTimeStr); 
                }
                catch (Exception e) {
                    System.out.println("ERROR: Could not parse date");
                    System.exit(1);
                }              
                
                // Parse the kitchen usage
                double kitchUsage = Double.parseDouble(splits[6]);
                MyData curData = new MyData(date, kitchUsage);
                
                if (myQ.size() == 5) {                    
                    MyData first = myQ.remove();
                    Double firstUsage = first.getPower();
                    Double diff = kitchUsage - firstUsage;
                    map.put(diff, new UsagePair(first, curData));                    
                }
                
                myQ.add(curData);                             
           }
                           
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<Double> keySet = map.keySet();
            List<Double> keys = new ArrayList(keySet);
                          
            Collections.sort(keys);
            Collections.reverse(keys);
            // Emit top 20 from list           
            for (int i = 0; i < 20 && i < keys.size(); i++) {
                //UsagePair myPair = map.get(keys.get(i));
                //context.write(new Text(Double.toString(keys.get(i))), new Text(myPair.toString()));
            }
            context.write(new Text("foo"), new Text("bar"));
        }
 
    }
    
    // Inputs: Key=<date,time>, Value=[ (<time1, <time1, powerVal1>), (<time1>, <time5, powerVal2>) ]
    // Outputs: 
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
        FileInputFormat.setInputPaths(job, new Path("./100.in")); 
        FileOutputFormat.setOutputPath(job, new Path("kitchenUse-output")); // put what you need as output
                
        // Set the map and reduce classes
        job.setMapperClass(KitchenMapper.class);
        job.setReducerClass(KitchenReducer.class);
        
        // Specify the output format classes
        job.setOutputKeyClass(Text.class); // specify the output class (what reduce() emits) for key
        job.setOutputValueClass(Text.class); // specify the output class (what reduce() emits) for value
        
         
        //job.setInputFormatClass(NLineInputFormat.class);
        //NLineInputFormat.addInputPath(job, new Path("./100.in"));
        //job.getConfiguration().setInt(
         //       "mapreduce.input.lineinputformat.linespermap", 1000); 
        
        
        job.setJobName("Household Kitchen Power");
                
        System.exit(job.waitForCompletion(true) ? 0 : 1);       
   }    
}

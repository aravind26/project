import java.io.*;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Applications
{
	
	public static class MapClass extends Mapper<LongWritable,Text,LongWritable,Text>
	   {

	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try{
	            String[] str = value.toString().split("\t");	 
	            long year = Long.parseLong(str[7]);
	            context.write(new LongWritable(year),new Text(str[0]));
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	      }
	   }
	
	
	
	
	 public static class ReduceClass extends Reducer<LongWritable,Text,LongWritable,LongWritable>
	   {
		    private LongWritable result = new LongWritable();
		    
		    public void reduce(LongWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		    
		    {
		    	long count=0;
		    	long yr = Long.parseLong(key.toString());
		        for (Text val : values)
		        {
		        	if(val != null)
		        	{
		        	count = count + 1;
		        	}
		        	
		        }
		        result.set(count);
		        context.write(key,result);
		    	
		    }
	   }

	   
	 public static void main(String[] args) throws Exception {
			    Configuration conf = new Configuration();
			    //conf.set("name", "value")
			    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
			    Job job = Job.getInstance(conf, "count");
			    job.setJarByClass(Applications.class);
			    job.setMapperClass(MapClass.class);
			    //job.setCombinerClass(ReduceClass.class);
			    job.setReducerClass(ReduceClass.class);
			   // job.setNumReduceTasks(0);
			    job.setMapOutputKeyClass(LongWritable.class);
			    job.setMapOutputValueClass(Text.class);
			    job.setOutputKeyClass(LongWritable.class);
			    job.setOutputValueClass(LongWritable.class);
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));
			    System.exit(job.waitForCompletion(true) ? 0 : 1);
			  }
	   

}


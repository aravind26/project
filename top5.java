import java.io.*;

import org.apache.hadoop.io.*;
import org.apache.hadoop.io.LongWritable.DecreasingComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;



public class top5
{
	
	public static class MapClass extends Mapper<LongWritable,Text,Text,Text>
	   {
	      public void map(LongWritable key, Text value, Context context)
	      {	    	  
	         try
	         {
	            String[] str = value.toString().split("\t");	 
	            if((str[1].toLowerCase()).equals("certified") && str[7].equals("2011"))
	            {
	            	String job_case_yr = str[8];
	            	String yrsite = str[7] + ";" + str[8];
	            
	            	context.write(new Text (job_case_yr),new Text (yrsite));
	       
	         
	         }
	         }
	         catch(Exception e)
	         {
	            System.out.println(e.getMessage());
	         }
	         
	      }
	   }
	
	public static class ReduceClass extends Reducer<Text,Text,LongWritable,Text>
	   {
		
		private LongWritable result = new LongWritable();
		private Text keyres = new Text();
		long max = 0;
		  	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
		  	{

		    	long count11=0;
		    	long count12=0;
		    	long count13=0;
		    	long count14=0;
		    	long count15=0;
		    	long count16=0;
		    	String city = "";
		    	String yrb[] = new String[2];
		    	
		    	//String yrb[] = key.toString().split(";");
		    	
		        for (Text val : values)
		        {
		        	 String yr[] = val.toString().split(";");
		        	 yrb[0] = yr[0];
		        	 yrb[1] = yr[1];
		        	//if(yr[0].equals("2011"))
		        		count11 = count11 + 1;
		        	//if(yr[0].equals("2012"))
		        		//count12 = count12 + 1;
		        	//if(yr[0].equals("2013"))
		        		//count13 = count13 + 1;
		        	//if(yr[0].equals("2014"))
		        		//count14 = count14 + 1;
		        	//if(yr[0].equals("2015"))
		        		//count15 = count15 + 1;
		       // 	if(yr[0].equals("2016"))
		        //		count16 = count16 + 1;
		        			
		        }
		        String redval = key + ";" + yrb[0];
		       
		        //if(yrb[0].equals("2011")) 
		      context.write(new LongWritable(count11), new Text(key));
		        //if(yrb[0].equals("2012")) 
				      // context.write(new LongWritable(count12),key);
		        //if(yrb[0].equals("2013")) 
				       //context.write(new LongWritable(count13),key);
		        //if(yrb[0].equals("2014")) 
				  //     context.write(new LongWritable(count11),new Text(redval));
		        //if(yrb[0].equals("2015")) 
				       //context.write(new LongWritable(count11),new Text(redval));
		        //if(yrb[0].equals("2016")) 
				   //    context.write(new LongWritable(count11),new Text(redval));

 
		    }
	   }
	public static class SortMapper extends Mapper<LongWritable,Text,LongWritable,Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String[] valueArr = value.toString().split("\t");
			context.write(new LongWritable(Long.parseLong(valueArr[0])), new Text(valueArr[1]));
		}
	}

	public static class SortReducer extends Reducer<LongWritable,Text,Text,LongWritable>
	{
		int counter = 0;
		public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException
		{
			
			
			//counter = 0;
			for(Text val : value)
			{
				if(counter < 5)
				
				context.write(new Text(val), key);
				
				counter++;
			}
			
			
		}
	}
	
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    //conf.set("name", "value")
		    //conf.set("mapreduce.input.fileinputformat.split.minsize", "134217728");
		    Job job = Job.getInstance(conf, "count");
		    job.setJarByClass(top5.class);
		    job.setMapperClass(MapClass.class);
		    //job.setCombinerClass(ReduceClass.class);
		    job.setReducerClass(ReduceClass.class);
		   //job.setNumReduceTasks(0);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(Text.class);
		    job.setInputFormatClass(TextInputFormat.class);
			
		      job.setOutputFormatClass(TextOutputFormat.class);
		    job.setOutputKeyClass(LongWritable.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    Path outputPath1 = new Path("FirstMapper");
			FileOutputFormat.setOutputPath(job, outputPath1);
			FileSystem.get(conf).delete(outputPath1, true);
			job.waitForCompletion(true);
		    
		    
		    
		    
		    Job job3 = Job.getInstance(conf,"Per Occupation - totalTxn");
			job3.setJarByClass(top5.class);
			job3.setSortComparatorClass(DecreasingComparator.class);
			job3.setMapperClass(SortMapper.class);
			//job3.setPartitionerClass(CaderPartitioner.class);
			job3.setReducerClass(SortReducer.class);
			//job3.setNumReduceTasks(0);
			job3.setSortComparatorClass(DecreasingComparator.class);
			job3.setMapOutputKeyClass(LongWritable.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(LongWritable.class);
			FileInputFormat.addInputPath(job3, outputPath1);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
		    System.exit(job3.waitForCompletion(true) ? 0 : 1);
		  }


}

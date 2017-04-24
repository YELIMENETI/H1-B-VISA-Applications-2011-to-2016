import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


//1 a) Is the number of petitions with Data Engineer job title increasing over time?

		
public class Que1_a {
	public static class pet_Mapper extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String[] str = value.toString().split("\t");
			
			String year = str[7];
			String job_title=str[4];
			if(job_title.equals("DATA ENGINEER"))
			{
			context.write(new Text(year), new IntWritable(1));
			}
		}
	}

	public static class reduc_dat extends Reducer<Text,IntWritable,Text,IntWritable>
	{
		public void reduce(Text key,Iterable<IntWritable> value,Context con) throws IOException, InterruptedException
		{
			int count=0;
			for(IntWritable val:value)
			{
				count++;
			}
			con.write(key, new IntWritable(count));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration con=new Configuration();
		Job job =Job.getInstance(con,"");
		job.setJarByClass(Que1_a.class);
		job.setMapperClass(pet_Mapper.class);
		job.setReducerClass(reduc_dat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
}

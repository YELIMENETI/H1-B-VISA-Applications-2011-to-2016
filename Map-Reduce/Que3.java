import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//3)Which industry has the most number of Data Scientist positions?

public class Que3 {

	public static class dataMapp extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");

			String job_title = str[4];
			String petition = str[3];
			if (job_title.equals("DATA SCIENTIST")) {
				con.write(new Text(petition), new Text(job_title));
			}
		}
	}

	public static class pet_Reducer extends
			Reducer<Text, Text, NullWritable, Text> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			int count = 0;
			for (Text val : values) {
				count++;
			}

			String mytotal = String.format("%d", count);
			String myValue = key.toString();
			myValue = myValue + ',' + mytotal;

			repToRecordMap.put(new Long(count), new Text(myValue));
			if (repToRecordMap.size() > 5) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Text t : repToRecordMap.descendingMap().values()) {
				context.write(NullWritable.get(), t);
			}
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "");
		job.setJarByClass(Que3.class);
		job.setMapperClass(dataMapp.class);

		job.setReducerClass(pet_Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

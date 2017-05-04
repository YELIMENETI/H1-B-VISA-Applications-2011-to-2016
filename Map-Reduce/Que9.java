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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//9) Which are top ten employers who have the highest success rate in petitions?
		
public class Que9 {

	public static class map_grw extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value, Context con)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			String employer_name = str[2];
			String application = (str[6]);
			if (str[1].equals("CERTIFIED") || str[1].equals("CERTIFIED-WITHDRWAN")) {
				con.write(new Text(employer_name), new IntWritable(1));
			}
		}
	}

	public static class reduce_grwt extends

	Reducer<Text, IntWritable, NullWritable, Text> {
		private TreeMap<Long, Text> repToRecordMap = new TreeMap<Long, Text>();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum = sum + val.get();
			}
			String mytotal = String.format("%d", sum);
			String myValue = key.toString();
			myValue = myValue + ',' + mytotal;

			repToRecordMap.put(new Long(sum), new Text(myValue));
			if (repToRecordMap.size() > 10) {
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
		job.setJarByClass(Que9.class);
		job.setMapperClass(map_grw.class);

		job.setReducerClass(reduce_grwt.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

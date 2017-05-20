package BigData;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PaloAlto {
	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
		String categories;
		String[] categoryList;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("::");
			if (line[1].contains("Palo Alto")) {
				categories = line[2].substring(5, line[2].length() - 1);
				categoryList = categories.split(",");
				for (String item : categoryList) {
					context.write(new Text(item.trim()), NullWritable.get());
				}
			}
		}
	}
	public static class Reduce extends Reducer<Text, IntWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// get all args
		if (otherArgs.length != 2) {
			System.err.println("Usage: PaloAlto <input:business.csv> <Output>");
			System.exit(2);
		}
		// create a job with name "PalAlto"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "PaloAlto");
		job.setJarByClass(PaloAlto.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(NullWritable.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

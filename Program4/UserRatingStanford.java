
package BigData;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

public class UserRatingStanford extends Configured implements Tool {

	static HashSet<String> bid = new HashSet<String>();
	// Mapper
	public static class DistributedMap extends Mapper<LongWritable, Text, Text, Text> {
		private Set<String> stopWordsSet = new HashSet<String>();
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			try {
				Configuration config = context.getConfiguration();
				String cacheFile = config.get("businessFile");
				Path path = new Path(cacheFile);
				FileSystem fileSystem = FileSystem.get(config);
				FileStatus[] fileStatus = fileSystem.listStatus(path);
				for (FileStatus status : fileStatus) {
				readFile(status,fileSystem);
				}
			} catch (Exception ex) {
				System.err.println("Exception in mapper setup: "+ ex.getMessage());
			}
		}
		private void readFile(FileStatus status, FileSystem fileSystem) {
			try {
				Path path = status.getPath();
				BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
				String line = reader.readLine();
				while (line != null) {
					String[] data = line.split("::");
					if (data[1].contains("Stanford")) {
						bid.add(data[0]);
					}
					line = reader.readLine();
				}

			} catch (Exception e) {
				System.err.println("Exception while reading stop words file: "+ e.getMessage());
			}
		}

		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException{
			String[] data = value.toString().split("::");
			if (bid.contains(data[2])) {
				context.write(new Text(data[1]), new Text(data[3]));
			}			
		}
	}
	
	// Driver
	public static void main(String[] args) throws Exception {
		//int exitCode = ToolRunner.run(new dcEg(), args);
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("businessFile", otherArgs[1]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "UserRating");
		job.setJarByClass(UserRatingStanford.class);
		job.setMapperClass(DistributedMap.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("businessFile", otherArgs[1]);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "UserRating");
		job.setJarByClass(UserRatingStanford.class);
		job.setMapperClass(DistributedMap.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(new Path(args[2]))) {
			fs.delete(new Path(args[2]), true);
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		return 1;
	}

}


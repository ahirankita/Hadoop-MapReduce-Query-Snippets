package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenRatedBusiness {
	static TreeMap<Float, List<Text>> ratingIdMap = new TreeMap<Float, List<Text>>(Collections.reverseOrder());
	public static class TopRatingMap extends Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("::"); 
				context.write(new Text(line[2]), new FloatWritable(Float.parseFloat(line[3])));
		
		}
	}	
	public static class TopRatingReduce extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException {
			float sum = 0.0f,average =  0.0f;
			int count = 0;
			for (FloatWritable value : values) {
				sum += value.get();
				count++; 
			}
			 average = sum / count;
			if (ratingIdMap.containsKey(average)) {
				ratingIdMap.get(average).add(new Text(key.toString()));
			} else {
				List<Text> idList = new ArrayList<Text>();
				idList.add(new Text(key.toString()));
				ratingIdMap.put(average, idList);
			}
		}
		@Override
		protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)throws IOException, InterruptedException {
			int count=0;
			for(Entry<Float, List<Text>> entry : ratingIdMap.entrySet()) {
				if(count>10){
					break;
				}
				  FloatWritable result=new FloatWritable();
				  result.set(entry.getKey());
				  for (int i=0;i<entry.getValue().size();i++) {
						if (count >= 10)
							break;
						context.write(new Text(entry.getValue().get(i).toString()), result);
						count++;
				}
		}
	}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: TopTenBusiness <input:review.csv> <out>");
			System.exit(2);
		}
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "TopTenBusiness");
		job.setJarByClass(TopTenRatedBusiness.class);
		job.setMapperClass(TopRatingMap.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		job.setReducerClass(TopRatingReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
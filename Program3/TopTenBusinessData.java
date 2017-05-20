package BigData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenBusinessData {
	
	static TreeMap<Float, List<Text>> ratingIdMap = new TreeMap<Float, List<Text>>(Collections.reverseOrder());
	//Job 1  Mapper :Take the input of review .csv and emits the businessIds and ratings. 
	public static class RatingMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("::");
			context.write(new Text(line[2]), new FloatWritable(Float.parseFloat(line[3])));
		}
	}
	//Job 1 Reducer:Calcultes the average rating of the Busineess Id and emits Business ID and average rating of Top ten Business
	public static class RatingReduce extends Reducer<Text, FloatWritable, Text, Text> {
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
		protected void cleanup(Reducer<Text, FloatWritable, Text, Text>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Entry<Float, List<Text>> entry : ratingIdMap.entrySet()) {
				if (count > 10) {
					break;
				}
				FloatWritable result = new FloatWritable();
				result.set(entry.getKey());
				for (int i = 0; i < entry.getValue().size(); i++) {
					if (count >= 10)
						break;
					context.write(new Text(entry.getValue().get(i).toString()+":"),new Text(result.toString()));
					count++;
				}
			}
		}
	}

	// Job 2 Mapper 1:get the input from  reducer of Job1.
	public static class TopTenRatingMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().trim();
			String[] detail = line.split(":");
			String bId = detail[0].trim();
			String rating = detail[1].trim();
			context.write(new Text(bId), new Text("R:" + bId + ":" + rating));

		}
	}
	// Job 2 Mapper2:Maps the data of business.csv
	public static class BusinessDataMap extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line[] = value.toString().split("::");
			context.write(new Text(line[0].trim()), new Text("B:" + line[0].trim() + ":"+ line[1].trim() + ":" + line[2].trim()));

		}
	}
	//Job 2 Reducer:Joins the data emitted by Mapper1 and Mapper 2 based on the Business Ids
	public static class TopTenBusinessDetailReducer extends Reducer<Text, Text, Text, Text> {
		private ArrayList<String> topTenRatings = new ArrayList<String>();
		private ArrayList<String> businessData = new ArrayList<String>();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
			for (Text text : values) {
				String value = text.toString();
				if (value.startsWith("R")) {
					topTenRatings.add(value.substring(2));
				} else {
					businessData.add(value.substring(2));
				}
			}
		}
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (String topBusiness : topTenRatings) {
				for (String detail : businessData) {
					String[] ratingList = topBusiness.split(":");
					String[] businessDataList = detail.split(":");
					String job1BId = ratingList[0].trim();
					String job2BId = businessDataList[0].trim();
					if (job1BId.equals(job2BId)) {
						context.write(new Text(job1BId), new Text(businessDataList[1] + "\t" + businessDataList[2] + "\t"+ ratingList[1]));
					break;
				}
				}
			}
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		if (otherArgs.length != 4) {
			System.err.println("Usage: TopTenBusiness <input1:review.csv><input2:business.csv><output1><final otput>");
			System.exit(0);
		}
		Job job1 = Job.getInstance(config, "job1");
		job1.setJarByClass(TopTenBusinessData.class);
		job1.setMapperClass(RatingMapper.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(FloatWritable.class);
		job1.setReducerClass(RatingReduce.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
		if (job1.waitForCompletion(true)) {
			Configuration config2 = new Configuration();
			Job job2 = Job.getInstance(config2, "job2");
			job2.setJarByClass(TopTenBusinessData.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			MultipleInputs.addInputPath(job2, new Path(args[2]),TextInputFormat.class, TopTenRatingMap.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]),TextInputFormat.class, BusinessDataMap.class);
			job2.setReducerClass(TopTenBusinessDetailReducer.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));
			job2.waitForCompletion(true);
		}
	}
}
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class OutDegree {

	public static class OutDegreeMapper extends
			Mapper<Object, Text, Text,Text> {

		private final static Text one = new Text("one");
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			//System.out.println("Key: " + value);
			String[] newItr = value.toString().split("<>");
			word.set(newItr[0]);
			//System.out.println("Word:" + newItr[0]);
			context.write(one, word);
			}
		}
	

	public static class IntSumReducer extends
			Reducer<Text, Text, Text, Text> {

		HashMap<String, Integer> cache = new HashMap<String, Integer>();
		public void reduce(Text key, Iterable<Text>values,
				Context context) throws IOException, InterruptedException {
			
			int wikiFiles = 0;
			int countLinks = 0;
			for(Text val: values)
			{
				if(cache.containsKey(val.toString())==false)
				{
					cache.put(val.toString(), 1);
					wikiFiles +=1;
				}
				countLinks+=1;
			}
			Text result = new Text();
			float degree = (float)countLinks/wikiFiles;
			result.set(Float.toString(degree));
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: outdegree <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "out degree");
		job.setJarByClass(OutDegree.class);
		job.setMapperClass(OutDegreeMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


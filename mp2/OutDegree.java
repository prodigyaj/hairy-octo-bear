import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OutDegree {

	public static class OutDegreeMapper extends
	Mapper<NullWritable, Text, Text, IntWritable> {

		public void map(NullWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String filename = ((FileSplit) context.getInputSplit()).getPath()
			 .getName();
			// Text fileKey = new Text(filename.toString());
			Text one = new Text("one");
			IntWritable number = new IntWritable();
			int countLinks = 0;
			for (int i = 0; i < value.getLength(); i++) {
				if (value.charAt(i) == 91) {
					if (i < value.getLength() - 1 && value.charAt(i + 1) == 91) {
						countLinks += 1;
						i += 2;
					}
				}
			}
			number.set(countLinks);
			//System.out.println(filename+":"+countLinks);
			context.write(one, number);
		}
	}

	public static class IntSumReducer extends
	Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			int n = 0;
			for (IntWritable val : values) {
				sum += val.get();
				n += 1;
			}
			//System.out.println(sum);
			//System.out.println("n:"+n);
			result.set(sum/n);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: Out Degree <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Out Degree");
		job.setJarByClass(OutDegree.class);
		job.setMapperClass(OutDegreeMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}

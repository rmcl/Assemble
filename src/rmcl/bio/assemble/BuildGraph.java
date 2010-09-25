package rmcl.bio.assemble;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import rmcl.bio.util.StringKmerTokenizer;


public class BuildGraph {
	public static class BuildDeBruijnGraphMapper 
			extends Mapper<LongWritable, Text, Text, IntWritable>{
  
		private final static IntWritable one = new IntWritable(1);
		private final static Text kmer = new Text();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
			
			StringKmerTokenizer itr = new StringKmerTokenizer(value.toString(), k);

			while (itr.hasMoreElements()) {
				kmer.set(itr.nextElement());
				context.write(kmer, one);
			}
		}
	}
	
	public static class BuildDeBruijnGraphReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BuildDeBruijnGraph <input path> <output path>");
			System.exit(1);
		}
		
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", "4");
		
		job.setJarByClass(BuildGraph.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BuildDeBruijnGraphMapper.class);
		job.setReducerClass(BuildDeBruijnGraphReducer.class);
		
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}

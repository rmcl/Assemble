package rmcl.bio.assemble;

import java.io.IOException;

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


public class BuildDeBruijnGraph {
	public static class BuildDeBruijnGraphMapper 
			extends Mapper<LongWritable, Text, BytesWritable, IntWritable>{
  
		private final static IntWritable one = new IntWritable(1);
		private final static BytesWritable kmer = new BytesWritable();
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
			
			KmerTokenizer itr = new KmerTokenizer(value.toString(), k);

			while (itr.hasMoreElements()) {
				byte[] q = itr.nextElement();
				kmer.set(q, 0 , q.length);
				context.write(kmer, one);
			}
		}
	}
	
	public static class BuildDeBruijnGraphReducer extends Reducer<BytesWritable,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context) 
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(new Text(key.toString()), result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BuildDeBruijnGraph <input path> <output path>");
			System.exit(1);
		}
		
		Job job = new Job();
		job.setJarByClass(BuildDeBruijnGraph.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BuildDeBruijnGraphMapper.class);
		job.setReducerClass(BuildDeBruijnGraphReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}

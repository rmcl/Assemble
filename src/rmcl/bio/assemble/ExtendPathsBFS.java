package rmcl.bio.assemble;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import rmcl.bio.util.StringKmerTokenizer;

public class ExtendPathsBFS {

	public static class ExtendPathsMapper 
			extends Mapper<Text, IntWritable, Text, Text>{
	  
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
		
	public static class ExtendPathsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
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
}

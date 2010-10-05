package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphCombiner;
import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphMapper;
import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphReducer;
import rmcl.bio.util.StringKmerTokenizer;
import rmcl.bio.util.input.FastaInputFormat;
import rmcl.bio.util.io.EulerianPath;
import rmcl.bio.util.io.KmerEdge;

public class SequencePathToText {

	public static class SequencePathToTextMapper extends Mapper<NullWritable, EulerianPath, NullWritable, EulerianPath>{
	  		
		public void map(NullWritable key, EulerianPath value, Context context) throws IOException, InterruptedException {	
			context.write(NullWritable.get(), value);
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: SequencePathToText <input path> <output path>");
			System.exit(1);
		}
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		
		job.setJarByClass(BuildGraph.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		FastaInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(SequencePathToTextMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(EulerianPath.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EulerianPath.class);
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 1 : 0);
	}
}

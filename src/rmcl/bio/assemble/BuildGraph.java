package rmcl.bio.assemble;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import rmcl.bio.util.StringKmerTokenizer;
import rmcl.bio.util.input.FastaInputFormat;
import rmcl.bio.util.io.GraphNode;
import rmcl.bio.util.io.KmerText;


public class BuildGraph {
	public static class BuildDeBruijnGraphMapper 
			extends Mapper<Text, Text, KmerText, GraphNode>{
  
		private final static KmerText kmer = new KmerText();
		private final static GraphNode node = new GraphNode();
		private final static Text[] r = new Text[1];
		
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
						
			// Extract kmers of length k+1. Each node represent edge between kmers.
			// The additional base pair will form to second kmer.
			StringKmerTokenizer itr = new StringKmerTokenizer(value.toString(), k + 1);

			while (itr.hasMoreElements()) {
				String seq = itr.nextElement();
				kmer.set(seq.substring(0, k));
				r[0] = key;
				node.setSequence(seq);
				node.setReadLabels(r);
				context.write(kmer, node);
			}
		}
	}
	
	public static class BuildDeBruijnGraphReducer extends Reducer<KmerText,GraphNode,NullWritable,GraphNode> {
		
		public void reduce(KmerText key, Iterable<GraphNode> values, Context context) 
				throws IOException, InterruptedException {
			Iterator<GraphNode> itr = values.iterator();
			GraphNode result = itr.next();
			
			while(itr.hasNext()) {
				result.addReadLabels(itr.next().getReadLabels());
			}	
			context.write(NullWritable.get(), result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: BuildDeBruijnGraph <input path> <output path>");
			System.exit(1);
		}
		
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", "25");
		
		job.setJarByClass(BuildGraph.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FastaInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(BuildDeBruijnGraphMapper.class);
		job.setReducerClass(BuildDeBruijnGraphReducer.class);
		
		job.setMapOutputKeyClass(KmerText.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(GraphNode.class);
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}

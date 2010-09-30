package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import rmcl.bio.util.io.GraphNode;
import rmcl.bio.util.io.KmerText;

public class ExtendPathsBFS {

	static final Log LOG = LogFactory.getLog(Job.class);
	
	public static class ExtendPathsMapper 
			extends Mapper<NullWritable, GraphNode, KmerText, GraphNode>{
	  
		private final static KmerText kmer = new KmerText();
			
		public void map(NullWritable key, GraphNode value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
			
			kmer.set(value.getFirstKmer(k));
			context.write(kmer, value);
			kmer.set(value.getLastKmer(k));
			context.write(kmer, value);
		}
	}
		
	public static class ExtendPathsReducer extends Reducer<KmerText,GraphNode,NullWritable,GraphNode> {
		private GraphNode result = new GraphNode();
			
		public void reduce(KmerText key, Iterable<GraphNode> values, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));

			// Hadoop values iterator can only be iterate through once apparently.
			// https://issues.apache.org/jira/browse/HADOOP-475
			// Hacky solution follows - This should be okay because we should have very few values per key.
			List<GraphNode> nodes = new ArrayList<GraphNode>();
			for (GraphNode x: values) {
				nodes.add(new GraphNode(x));
			}

			int count = 0;
			for (GraphNode x: nodes) {
				String xf = x.getFirstKmer(k);
				String xl = x.getLastKmer(k);
				
				for (GraphNode y: nodes) {
					if (x == y) {
						continue;
					}
					
					String yf = y.getFirstKmer(k);
					String yl = y.getLastKmer(k);
								
					if (xf.compareTo(yl) == 0) {
						result.clear();
						
						String s1 = y.sequence.toString();
						String s2 = x.sequence.toString();
						
						String newSeq = s1.substring(0, s1.length() - k) + s2;
						
						result.setSequence(newSeq);
						result.addReadLabels(x.getReadLabels());
						result.addReadLabels(y.getReadLabels());
						context.write(NullWritable.get(), result);
					}
				}
			}
			
			//LOG.info(key.toString()+": survivors: "+count);
			
		}
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.println("Usage: ExtendPathsBFS <input path> <output path> <textout>");
			System.exit(1);
		}
		
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", "25");
		
		job.setJarByClass(ExtendPathsMapper.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		
		if (args[2].compareTo("true") != 0) {
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
		}

		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(ExtendPathsMapper.class);
		job.setReducerClass(ExtendPathsReducer.class);
		
		job.setMapOutputKeyClass(KmerText.class);

		job.setOutputKeyClass(NullWritable.class);
		//job.setOutputKeyClass(KmerText.class);
		job.setOutputValueClass(GraphNode.class);
		
		System.exit(job.waitForCompletion(true) ? 0: 1);
	}
}

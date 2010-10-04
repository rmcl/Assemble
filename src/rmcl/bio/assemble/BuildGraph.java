package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import rmcl.bio.util.io.KmerEdge;
import rmcl.bio.util.io.KmerLabel;
import rmcl.bio.util.io.EulerianPath;



public class BuildGraph {
	public static class BuildDeBruijnGraphMapper 
			extends Mapper<Text, Text, Text, EulerianPath>{
  
		private final static EulerianPath path = new EulerianPath();
		private final static KmerEdge edge = new KmerEdge();
		private final static List<KmerEdge> edges = new ArrayList<KmerEdge>();
		private final static KmerLabel label = new KmerLabel();
		private final static Set<KmerLabel> labels = new HashSet<KmerLabel>();		
		private final static Text kmerText = new Text();
		
		public BuildDeBruijnGraphMapper() {
			labels.add(label);	
			edges.add(edge);
			path.set(edges);
		}
		
		public void map(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
						
			// Extract kmers of length k+1. Each node represent edge between kmers.
			// The additional base pair will form to second kmer.
			StringKmerTokenizer itr = new StringKmerTokenizer(value.toString(), k + 1);
			
			while (itr.hasMoreElements()) {				
				label.set(key.toString(), itr.getIndex());
				String seq = itr.nextElement();
				edge.set(seq, labels);
				kmerText.set(seq);
				
				context.write(kmerText, path);
			}
		}
	}

	public static class BuildDeBruijnGraphCombiner extends Reducer<Text,EulerianPath,Text,EulerianPath> {
		
		public void reduce(Text key, Iterable<EulerianPath> values, Context context) 
				throws IOException, InterruptedException {
			Iterator<EulerianPath> itr = values.iterator();
			EulerianPath result = new EulerianPath(itr.next());
			
			while(itr.hasNext()) {
				result.get(0).addLabels((itr.next().get(0).labels()));
			}	
			context.write(key, result);
		}
	}
	
	public static class BuildDeBruijnGraphReducer extends Reducer<Text,EulerianPath,NullWritable,EulerianPath> {
		
		public void reduce(Text key, Iterable<EulerianPath> values, Context context) 
				throws IOException, InterruptedException {
			Iterator<EulerianPath> itr = values.iterator();
			EulerianPath result = new EulerianPath(itr.next());
			
			while(itr.hasNext()) {
				result.get(0).addLabels((itr.next().get(0).labels()));
			}	
			context.write(NullWritable.get(), result);
		}
	}
	
}

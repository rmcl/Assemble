package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import rmcl.bio.util.io.EulerianPath;

public class ExtendPathsBFS {

	static final Log LOG = LogFactory.getLog(Job.class);
	
	public static class ExtendPathsMapper 
			extends Mapper<NullWritable, EulerianPath, Text, EulerianPath>{
	  
		private final static Text kmer = new Text();
			
		public void map(NullWritable key, EulerianPath value, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
			
			kmer.set(value.firstKmer(k));
			context.write(kmer, value);
			kmer.set(value.lastKmer(k));
			context.write(kmer, value);
		}
	}
		
	public static class ExtendPathsReducer extends Reducer<Text,EulerianPath,NullWritable,EulerianPath> {
			
		public void reduce(Text key, Iterable<EulerianPath> values, Context context) 
				throws IOException, InterruptedException {
			
			int k = Integer.parseInt(context.getConfiguration().get("kmer-length"));
			double minCoverage = (double) context.getConfiguration().getFloat("minimum-coverage", 0);
			
			// Hadoop values iterator can only be iterate through once apparently.
			// https://issues.apache.org/jira/browse/HADOOP-475
			// Hacky solution follows - This should be okay because we should have very few values per key.
			// Put them in a map indexed by sequence so that there will be no duplicates.
			Map<String,EulerianPath> nodes = new HashMap<String,EulerianPath>();
			for (EulerianPath x: values) {
				nodes.put(x.toString(), new EulerianPath(x));
			}
			System.err.println("Reducer received "+nodes.size()+" paths.");

			for (Map.Entry<String,EulerianPath> xx: nodes.entrySet()) {
				EulerianPath x = xx.getValue();
				
				if (x.averageConverage() < minCoverage) {
					continue;
				}
				
				for (Map.Entry<String, EulerianPath> yy: nodes.entrySet()) {
					EulerianPath y = yy.getValue();
					
					if (x == y) {
						continue;
					}
					
					if (y.averageConverage() < minCoverage) {
						continue;
					}					
					
					EulerianPath result = x.extendPath(y, k);
					if (result != null) {
						context.write(NullWritable.get(), result);
					}
				}
			}		
			//LOG.info(key.toString()+": survivors: "+count);
		}
	}
}

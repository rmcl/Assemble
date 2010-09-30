package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphMapper;
import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphReducer;
import rmcl.bio.util.input.FastaInputFormat;
import rmcl.bio.util.io.GraphNode;
import rmcl.bio.util.io.KmerText;

public class Assemble {
	private int kmerLength;
	private List<String> inputPaths;
	private String outputPath;
	
	private final String BUILD_OUTPUT_DIR="/graph";
	
	public Assemble(String outPath, int kmerLen) throws IOException {
		inputPaths = new ArrayList<String>();
		kmerLength = kmerLen;
		outputPath = outPath;
	}
	
	private Job setupBuildGraphJob(String outputPath) throws IOException {
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", Integer.toString(kmerLength));
		
		job.setJarByClass(BuildGraph.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		for (String p: inputPaths) {
			FastaInputFormat.addInputPath(job, new Path(p));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(BuildDeBruijnGraphMapper.class);
		job.setReducerClass(BuildDeBruijnGraphReducer.class);
		
		job.setMapOutputKeyClass(KmerText.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(GraphNode.class);
		
		return job;
	}
	
	public void addInputDir(String path) {
		inputPaths.add(path);		
	}
	
	public void run() throws IOException, InterruptedException, ClassNotFoundException {
		
		Job build = setupBuildGraphJob(outputPath+BUILD_OUTPUT_DIR);
		System.exit(build.waitForCompletion(true) ? 0: 1);		
		
		
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: BuildDeBruijnGraph <input path> <output path>");
			System.exit(1);
		}
		
		Assemble asm = new Assemble(args[1], 25);
		asm.addInputDir(args[0]);
		
		asm.run();
	}
}

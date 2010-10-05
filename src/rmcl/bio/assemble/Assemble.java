package rmcl.bio.assemble;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphCombiner;
import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphMapper;
import rmcl.bio.assemble.BuildGraph.BuildDeBruijnGraphReducer;
import rmcl.bio.assemble.ExtendPathsBFS.ExtendPathsMapper;
import rmcl.bio.assemble.ExtendPathsBFS.ExtendPathsReducer;
import rmcl.bio.util.input.FastaInputFormat;
import rmcl.bio.util.io.EulerianPath;


public class Assemble {
	private int kmerLength;
	private List<String> inputPaths;
	private String outputPath;
	private int extendIter;
	
	private final String BUILD_OUTPUT_DIR="/graph";
	
	public Assemble(String outPath, int kmerLen) throws IOException {
		inputPaths = new ArrayList<String>();
		kmerLength = kmerLen;
		outputPath = outPath;
		extendIter = 3;
	}
	
	private Job setupBuildGraphJob(String outputPath) throws IOException {
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", Integer.toString(kmerLength));

		//Increase task jvm memory
		config.set("mapred.child.java.opts", "-Xmx2000m");
		
		//Compress map output
		//These settings seem to make the map stage take WAY longer. Like 15 minutes longer.
		//config.setBoolean("mapred.compress.map.output", true);
		//config.setClass("mapred.map.output.compression.codec", GzipCodec.class, CompressionCodec.class);
		
		job.setJarByClass(BuildGraph.class);
		job.setInputFormatClass(FastaInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		for (String p: inputPaths) {
			FastaInputFormat.addInputPath(job, new Path(p));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		job.setMapperClass(BuildDeBruijnGraphMapper.class);
		job.setCombinerClass(BuildDeBruijnGraphCombiner.class);
		job.setReducerClass(BuildDeBruijnGraphReducer.class);
		job.setNumReduceTasks(10);
		

		
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EulerianPath.class);
		
		return job;
	}
	
	public Job setupExtendPathJob() throws IOException {
		Job job = new Job();
		
		Configuration config = job.getConfiguration();
		config.set("kmer-length", Integer.toString(kmerLength));
		config.set("mapred.job.name", "ExtendPath-iter-"+extendIter);
		//Increase task jvm memory
		config.set("mapred.child.java.opts", "-Xmx2000m");
		
		config.setFloat("minimum-coverage", (float)10.0);
		
		job.setJarByClass(ExtendPathsBFS.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		if (extendIter == 0) {
			FileInputFormat.addInputPath(job, new Path(outputPath+BUILD_OUTPUT_DIR));
		} else {
			FileInputFormat.addInputPath(job, new Path(outputPath+"/extend-"+extendIter));
		}
		FileOutputFormat.setOutputPath(job, new Path(outputPath+"/extend-"+(extendIter+1)));
		
		job.setMapperClass(ExtendPathsMapper.class);
		job.setReducerClass(ExtendPathsReducer.class);
		job.setNumReduceTasks(200);
		
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(EulerianPath.class);
		
		extendIter++;
		return job;
	}
	
	public void addInputDir(String path) {
		inputPaths.add(path);		
	}
	
	public void run() throws IOException, InterruptedException, ClassNotFoundException {
		
		/*
		Job build = setupBuildGraphJob(outputPath+BUILD_OUTPUT_DIR);
	
		if (build.waitForCompletion(true) == false) {
			return;
		}*/
		
		for (int i = 0; i < 10; i++) {
			Job b = setupExtendPathJob();
			if (b.waitForCompletion(true) == false) {
				break;		
			}
		}
	}
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if (args.length != 2) {
			System.err.println("Usage: BuildDeBruijnGraph <input path> <output path>");
			System.exit(1);
		}
		
		Assemble asm = new Assemble(args[1], 31);
		asm.addInputDir(args[0]);
		
		asm.run();
	}
}

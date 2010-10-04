package test.rmcl.bio.util.input;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.fs.FileSystem;
import rmcl.bio.util.input.FastaInputFormat;
import rmcl.bio.util.input.FastaRecordReader;

public class FastaRecordReaderTest extends TestCase {
  
  private static Configuration conf = new Configuration();
  private static FileSystem localFs = null; 

  InputSplit split;
  TaskAttemptContext context;
  
  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }
  
  public void setUp() throws Exception {
    Job job = new Job(conf);
    Path file = new Path("/tmp/test.fa");

    localFs.delete(file, true);
    FileInputFormat.setInputPaths(job, file); 

    Writer writer = new OutputStreamWriter(localFs.create(file));;
    for (int length = 0; length < 10; length++) {
      writer.write("> lala"+length+"\n");
      for (int i = 0; i < 16; i++) {
    	  writer.write("ATG");
          
      }
      writer.write("\n");
      
    }
    writer.close();
    
    InputSplit split = new FileSplit(new Path("/tmp/test.fa"),0, 5000, null);
    TaskAttemptID id = new TaskAttemptID();
    TaskAttemptContext context = new TaskAttemptContext(job.getConfiguration(), id);
  }
  
  public void testReaderOutputsAllRecords() throws Exception {
	  this.setUp();
	  
	  RecordReader r = new FastaRecordReader();	  
      r.initialize(split, context);
  }
  
}
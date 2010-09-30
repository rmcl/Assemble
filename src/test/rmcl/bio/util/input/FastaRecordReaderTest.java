package test.rmcl.bio.util.input;

import java.io.*;
import java.util.*;
import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.FileSystem;
import rmcl.bio.util.input.FastaInputFormat;

public class FastaRecordReaderTest extends TestCase {
  
  private static Configuration conf = new Configuration();
  private static FileSystem localFs = null; 

  static {
    try {
      localFs = FileSystem.getLocal(conf);
    } catch (IOException e) {
      throw new RuntimeException("init failure", e);
    }
  }

  private static Path workDir = 
    new Path(new Path(System.getProperty("test.build.data", "."), "data"),
             "TestNLineInputFormat");
  
  public void setUp() throws Exception {
    Job job = Job.getInstance(conf);
    Path file = new Path(workDir, "test.txt");

    int seed = new Random().nextInt();
    Random random = new Random(seed);

    localFs.delete(workDir, true);
    FileInputFormat.setInputPaths(job, workDir); 

    for (int length = 0; length < 10; length++) {

      Writer writer = new OutputStreamWriter(localFs.create(file));
      writer.write("> lala"+length);
      try {
        for (int i = 0; i < 16; i++) {
          writer.write("ATG");
          
        }
        writer.write("\n");
      } finally {
        writer.close();
      }
    }
  }
  
  public void testReaderOutputsAllRecords() {
	  
  }
  
}
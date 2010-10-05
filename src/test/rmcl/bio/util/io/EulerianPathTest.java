package test.rmcl.bio.util.io;

import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import rmcl.bio.util.io.KmerEdge;
import rmcl.bio.util.io.EulerianPath;
import rmcl.bio.util.io.KmerLabel;

import junit.framework.TestCase;

public class EulerianPathTest extends TestCase {
	  public void setUp() throws Exception {
		  
	  }
		  
	  public void testGetFirstKmer() {
		  List<KmerEdge> edges = new ArrayList<KmerEdge>();
		  for (int i = 0; i < 5; i++) {
			  KmerEdge e = new KmerEdge();
			  e.set("AAB", 1);
			  edges.add(e);
		  }
		  
		  EulerianPath p = new EulerianPath();
		  p.set(edges);
		  
		  assertEquals(p.firstKmer(7), "AABAABA");
		  assertEquals(p.firstKmer(25), null);
	  }
	  
	  public void testGetLastKmer() {
		  List<KmerEdge> edges = new ArrayList<KmerEdge>();
		  for (int i = 0; i < 5; i++) {
			  KmerEdge e = new KmerEdge();
			  e.set("AAB", 1);
			  edges.add(e);
		  }
		  
		  EulerianPath p = new EulerianPath();
		  p.set(edges);
		  
		  assertEquals("BAAB", p.lastKmer(4));
	  }
	  
	  public void testExtendPath() {
		  List<KmerEdge> edges = new ArrayList<KmerEdge>();
		  KmerEdge e = new KmerEdge();
		  e.set("AAB", 1);
		  edges.add(e);
		  EulerianPath p = new EulerianPath();
		  p.set(edges);
		  
		  List<KmerEdge> edges2 = new ArrayList<KmerEdge>();
		  KmerEdge e2 = new KmerEdge();
		  e2.set("ABC", 1);
		  edges2.add(e2);
		  EulerianPath p2 = new EulerianPath();
		  p2.set(edges2);
		  
		  EulerianPath p3 = p.extendPath(p2, 2);
		  assertNotNull(p3);
		  assertEquals(p3.toString(), "2\tAAB,1\tABC,1");
	  }
	  
	  public void testAvgCoverage() {
		  List<KmerEdge> edges = new ArrayList<KmerEdge>();
		  KmerEdge e = new KmerEdge();
		  e.set("AAB", 2);
		  edges.add(e);
		  EulerianPath p = new EulerianPath();
		  p.set(edges);
		  
		  
		  edges = new ArrayList<KmerEdge>();
		  e = new KmerEdge();

		  e.set("ABC", 1);
		  edges.add(e);
		  EulerianPath p2 = new EulerianPath();
		  p2.set(edges);
		  
		  EulerianPath p3 = p.extendPath(p2, 2);
		  assertEquals(1.5, p3.averageConverage());
	  }
	  
	  public void testGetSequence() {
		  List<KmerEdge> edges = new ArrayList<KmerEdge>();
		  KmerEdge e = new KmerEdge();
		  e.set("CAAB", 2);
		  edges.add(e);
		  EulerianPath p = new EulerianPath();
		  p.set(edges);
		  
		  
		  edges = new ArrayList<KmerEdge>();
		  e = new KmerEdge();

		  e.set("ABCD", 1);
		  edges.add(e);
		  EulerianPath p2 = new EulerianPath();
		  p2.set(edges);
		  
		  EulerianPath p3 = p.extendPath(p2, 2);
		  
		  assertEquals("CAABD", p3.getSequence());
	  }
}

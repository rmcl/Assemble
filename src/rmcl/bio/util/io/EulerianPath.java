package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * Defines a path through a number of edges in the DeBruijn Graph.
 * @author rmcl@cs.ucsb.edu (Russell McLoughlin)
 *
 */
public class EulerianPath implements Writable {

	private List<KmerEdge> edges;
	
	public EulerianPath() {
		edges = null;
	}
	
	/**
	 * Create a shallow copy of the given path
	 * @param p The path to copy from.
	 */
	public EulerianPath(EulerianPath p) {
		edges = new ArrayList<KmerEdge>();
		edges.addAll(p.edges);
	}
	
	/**
	 * Attempt to extend the path with the given path. 
	 * Checks if the last kmer of this path matches with 
	 * the first kmer of passed kmer.
	 * @param p The path to attempt to merge with
	 * @return The new combined path.
	 */
	public EulerianPath extendPath(EulerianPath p, int k) {
		if (lastKmer(k).compareTo(p.firstKmer(k)) == 0) { 
			EulerianPath newPath = new EulerianPath(this);
			newPath.edges.addAll(p.edges);
			return newPath;
		}
		return null;
	}
	
	public double averageConverage() {
		float cov = 0;
		for (KmerEdge e: edges) {
			cov += e.coverage();
		}
		return cov / edges.size();
	}
	
	/**
	 * Return the first kmer of length k in the path
	 * @param k The length of the mer.
	 * @return A string representation of the kmer.
	 */
	public String firstKmer(int k) {
		StringBuffer buffer = new StringBuffer();
	
		for (int i = 0, t = k; t > 0; i++) {
			if (i >= edges.size()) {
				return null;
			}
			
			KmerEdge e = edges.get(i);
			
			if ((t - e.sequence.length()) >= 0) {
				buffer.append(e.sequence);
				t -= e.sequence.length();
			} else {
				buffer.append(e.sequence.substring(0, t));	
				break;
			}
		}
		return buffer.toString();
	}
	
	/**
	 * Return the last kmer of length k in the path.
	 * @param k The length of the mer.
	 * @return The kmer represented as a string.
	 */
	public String lastKmer(int k) {
		StringBuffer buffer = new StringBuffer();
		
		for (int i=edges.size()-1, t = k; t > 0 && i >= 0; i--) {
			KmerEdge e = edges.get(i);
			
			if ((t - e.sequence.length()) >= 0) {
				buffer.insert(0, e.sequence);
				t -= e.sequence.length();
			} else {
				buffer.insert(0, e.sequence.substring(e.sequence.length() - t));
				break;
			}
		}
		
		if (buffer.length() != k) {
			return null;
		}
		return buffer.toString();
	}
		
	/**
	 * Set the list of edges in this path
	 * @param e The new list of edges
	 */
	public void set(List<KmerEdge> e) {
		edges = e;
	}
	
	/**
	 * Get the list of edges.
	 * @return The list of edges.
	 */
	public List<KmerEdge> get() {
		return edges;
	}
	
	/**
	 * Get a particular edge in the path
	 * @param i The index of the edge.
	 * @return
	 */
	public KmerEdge get(int i) {
		if (edges == null) {
			return null;
		}
		return edges.get(i);
	}
	
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append(edges.size());
		for (KmerEdge e: edges) {
			b.append("\t");
			b.append(e.toString());
		}
		return b.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int edgeCount = in.readInt();
		edges = new ArrayList<KmerEdge>(edgeCount);
		for (int i = 0; i < edgeCount; i++) {
			edges.add(KmerEdge.read(in));
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		if (edges == null) {
			out.writeInt(0);
			return;
		}
		
		out.writeInt(edges.size());
		for (KmerEdge e: edges) {
			e.write(out);
		}
	}

}

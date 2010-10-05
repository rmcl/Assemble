package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Defines a single edge in the DeBruijn Graph.
 * @author rmcl@cs.ucsb.edu (Russell McLoughlin)
 *
 */
public class KmerEdge implements Writable {

	protected String sequence;
	protected int coverage;
	
	public void set(String sequence, int coverage) {
		this.sequence = sequence;
		this.coverage = coverage;
	}
	
	public void setCoverage(int newCov) {
		this.coverage = newCov;
	}
	
	public int coverage() {
		return coverage;
	}
	
	public static KmerEdge read(DataInput in) throws IOException {
		KmerEdge k = new KmerEdge();
		k.readFields(in);
		return k;
	}
	
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append(sequence);
		b.append(",");
		b.append(coverage);
		return b.toString();
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		sequence = in.readUTF();
		coverage = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sequence);
		out.writeInt(coverage);
	}

}

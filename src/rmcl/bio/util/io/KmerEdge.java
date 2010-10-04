package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Writable;

/**
 * Defines a single edge in the DeBruijn Graph.
 * @author rmcl@cs.ucsb.edu (Russell McLoughlin)
 *
 */
public class KmerEdge implements Writable {

	String sequence;
	Set<KmerLabel> labels;
	
	public void set(String sequence, Set<KmerLabel> labels) {
		this.sequence = sequence;
		this.labels = labels;
	}
	
	public Set<KmerLabel> labels() {
		return labels;
	}
	public void addLabels(Set<KmerLabel> l) {
		labels.addAll(l);
	}
	
	public int coverage() {
		return labels.size();
	}
	
	public String toString() {
		StringBuffer b = new StringBuffer();
		b.append(sequence);
		if (labels != null) {
			for (KmerLabel l: labels) {
				b.append(",");
				b.append(l.toString());
			}
		}
		return b.toString();
	}
	
	public static KmerEdge read(DataInput in) throws IOException {
		KmerEdge k = new KmerEdge();
		k.readFields(in);
		return k;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		sequence = in.readUTF();
		int labelCount = in.readInt();
		labels = new HashSet<KmerLabel>(labelCount);
		
		for (int i = 0; i < labelCount; i++) {
			labels.add(KmerLabel.read(in));
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(sequence);
		out.writeInt(labels.size());
		for (KmerLabel k: labels) {
			k.write(out);
		}
		
	}

}

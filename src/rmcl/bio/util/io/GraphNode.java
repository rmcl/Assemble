package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class GraphNode implements Writable {

	public Text sequence;
	protected Set<Text> readLabels;
	
	public GraphNode() {
		sequence = new Text();
		readLabels = new HashSet<Text>();
	}
	
	public GraphNode(GraphNode n) {
		sequence = new Text(n.sequence);
		readLabels = new HashSet<Text>();
		for (Text t: n.readLabels) {
			readLabels.add(new Text(t));
		}
	}
	
	public String getFirstKmer(int k) {
		String seq = sequence.toString();
		if (k > seq.length()) {
			return null;
		}
		return seq.substring(0, k);
	}
	
	public String getLastKmer(int k) {
		String seq = sequence.toString();
		int len = seq.length();
		if (k > len) {
			return null;
		}	
		return seq.substring(len - k, len);
	}

	public void clear() {
		sequence.clear();
		readLabels.clear();
	}
	
	public void setSequence(String seq) {
		sequence.set(seq);
	}
	
	public void setSequence(Text seq) {
		sequence.set(seq);
	}
	
	public void setReadLabels(Text[] labels) {
		readLabels.clear();
		for (Text l: labels) {
			readLabels.add(l);
		}
	}
		
	public Text[] getReadLabels() {
		return readLabels.toArray(new Text[readLabels.size()]);
	}
	
	public void addReadLabels(Text[] r) {
		for (Text l: r) {
			readLabels.add(l);
		}
	}
		
	@Override
	public void readFields(DataInput in) throws IOException {
		sequence.readFields(in);
		int c = in.readInt();
		readLabels.clear();
		while (c > 0) {
			readLabels.add(new Text(Text.readString(in)));
			c--;
		}
	}

	@Override
	public void write(DataOutput out) throws IOException {
		sequence.write(out);
		out.writeInt(readLabels.size());
		for (Text l: readLabels) {
			Text.writeString(out, l.toString());
		}
	}
	
	public String toString() {
		StringBuffer out = new StringBuffer();
		out.append(sequence);
		out.append("\t");
		for (Text r: readLabels) {
			out.append(r.toString());
			out.append(",");
		}
		return out.toString();
	}
}

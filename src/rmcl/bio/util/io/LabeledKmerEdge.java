package rmcl.bio.util.io;

import java.util.Set;

public class LabeledKmerEdge extends KmerEdge {

	Set<KmerLabel> labels;
	
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
	
}

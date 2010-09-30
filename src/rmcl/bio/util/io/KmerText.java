package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class KmerText implements WritableComparable<KmerText> {
	String kmer;

	public void set(String k) {
		kmer = k;
	}
	
	public String get() {
		return kmer;
	}

	public String toString() {
		return kmer;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		kmer = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(kmer);
	}

	@Override
	public int compareTo(KmerText o) {
		return kmer.compareTo(o.kmer);
	}
}

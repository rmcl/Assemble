package rmcl.bio.util.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class KmerLabel implements Writable {
	private String label;
	private int pos;
	
	public void set(String label, int pos) {
		this.label = label;
		this.pos = pos;
	}
	
	public String toString() {
		return "(" + pos + "," + label + ")";
	}
	
	public static KmerLabel read(DataInput in) throws IOException {
		KmerLabel k = new KmerLabel();
		k.readFields(in);
		return k;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		label = in.readUTF();
		pos = in.readInt();
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(label);
		out.writeInt(pos);
	}
}

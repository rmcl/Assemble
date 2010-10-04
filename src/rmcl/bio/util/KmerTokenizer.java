package rmcl.bio.util;

import java.math.BigInteger;
import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Enumeration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class KmerTokenizer implements Enumeration<byte[]> {

	private CharacterIterator it;
	private int kmerLength;
	private StringBuffer buffer;

	public KmerTokenizer(String k, int kmerLen) {
		it = new StringCharacterIterator(k);
		buffer = new StringBuffer();
		kmerLength = kmerLen;
	}
	

	/**
	 * Check if the kmer string has more kmers.
	 * @return Returns true if more kmers are available.
	 */
	public boolean hasMoreElements() {
		if (it.current() == CharacterIterator.DONE &&
				buffer.length() < kmerLength * 2) {
			return false;
		}
		return true;
		
	}

	/**
	 * Retrieve the next kmer from the sequence
	 * @return The kmer byte compressed.
	 * 		'A' - 00
	 * 		'T' - 01
	 * 		'G' - 10
	 * 		'C' - 11
	 */
	public byte[] nextElement() {
		
		for(char c = it.current(); c != CharacterIterator.DONE; c = it.next()) {
			if (buffer.length() == kmerLength * 2) {
				break;
			}
			switch (c) {
			case 'a':
			case 'A':
				buffer.append("00");
				continue;
			case 't':
			case 'T':
				buffer.append("01");
				continue;
			case 'g':
			case 'G':
				buffer.append("10");
				continue;
			case 'c':
			case 'C':
				buffer.append("11");
				continue;
			default:
				continue;
			}
		}
		
		if (buffer.length() < kmerLength * 2) {
			return null;
		}
		
		BigInteger bi = new BigInteger(buffer.toString(), 2);		
		buffer = new StringBuffer(buffer.substring(2));

		return bi.toByteArray();
	}
	
	public static String byteToString(byte[] in) {
		StringBuffer b = new StringBuffer();
		
//		for (byte b1: in) {
//			for (int i = 0; i < 8; i+= 2) {
//				b1 >> i
//			}
//		}
		
		
		return null;
		
	}
}

package rmcl.bio.util;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;
import java.util.Enumeration;


public class StringKmerTokenizer implements Enumeration<String> {

	private CharacterIterator it;
	private int kmerLength;
	private StringBuffer buffer;

	public StringKmerTokenizer(String k, int kmerLen) {
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
	 * 
	 */
	public String nextElement() {
		for(char c = it.current(); c != CharacterIterator.DONE; c = it.next()) {
			if (buffer.length() == kmerLength) {
				break;
			}
			buffer.append(c);
		}
		
		if (buffer.length() < kmerLength) {
			return null;
		}		
		String result = buffer.toString();
		buffer = new StringBuffer(buffer.substring(1));
		return result;
	}
}
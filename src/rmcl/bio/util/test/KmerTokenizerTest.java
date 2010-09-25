package rmcl.bio.util.test;

import java.math.BigInteger;

import org.apache.hadoop.io.Text;

import rmcl.bio.util.KmerTokenizer;
import junit.framework.TestCase;

public class KmerTokenizerTest extends TestCase {
	
	public void testTwoMer() {
		KmerTokenizer k = new KmerTokenizer("CATGC", 2);
	    
		assertEquals("1100", new BigInteger(k.nextElement()).toString(2));
		assertEquals("1", new BigInteger(k.nextElement()).toString(2));
		assertEquals("110", new BigInteger(k.nextElement()).toString(2));	
		assertEquals("1011", new BigInteger(k.nextElement()).toString(2));
		assertFalse(k.hasMoreElements());
		assertEquals(null, k.nextElement());
	}
	
	public void testTwoMerShouldBeRepresentedAsSingleByte() {
		KmerTokenizer k = new KmerTokenizer("CATGC", 2);
		assertEquals(1, k.nextElement().length);
		assertTrue(k.hasMoreElements());
	}
	
	public void testThreeMer() {
		KmerTokenizer k = new KmerTokenizer("CATGCAGCCAATGCATGCATCGGCCAAATTGCACTAGGAGAACGTAGACA", 3);
		assertEquals("110001", new BigInteger(k.nextElement()).toString(2));
	}
}

package rmcl.bio.util.test;

import rmcl.bio.util.KmerTokenizer;
import rmcl.bio.util.StringKmerTokenizer;
import junit.framework.TestCase;

public class StringKmerTokenizerTest extends TestCase {
	
	public void testTwoMer() {
		StringKmerTokenizer k = new StringKmerTokenizer("CATGC", 2);
	    
		assertEquals("CA", k.nextElement());
		assertEquals("AT", k.nextElement());
		assertEquals("TG", k.nextElement());
		assertEquals("GC", k.nextElement());
		assertFalse(k.hasMoreElements());
		assertEquals(null, k.nextElement());
	}
	
	public void testTwoMerShouldBeRepresentedAsSingleByte() {
		StringKmerTokenizer k = new StringKmerTokenizer("CATGC", 2);
		assertEquals(2, k.nextElement().length());
		assertTrue(k.hasMoreElements());
		
		StringKmerTokenizer z = new StringKmerTokenizer("CATGC", 4);
		assertEquals(4, z.nextElement().length());
	}
	
	public void testThreeMer() {
		StringKmerTokenizer k = new StringKmerTokenizer("CATGCAGCCAATGCATGCATCGGCCAAATTGCACTAGGAGAACGTAGACA", 3);
		assertEquals("CAT", k.nextElement());
	}
}

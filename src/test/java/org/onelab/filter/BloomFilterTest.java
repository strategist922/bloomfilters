package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Hash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BloomFilterTest {

	private BloomFilter bf = null;
	private static final int vectorSize = 8;
	private static final int numberHashFunctions = 2;
	
	@Before
	public void setUp() throws Exception {
		bf = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
	}

	@After
	public void tearDown() throws Exception {
		bf = null;
	}

	@Test
	public void testBloomFilterConstructor() {
		assertNotNull(bf);
		assertEquals(vectorSize, bf.vectorSize);
		assertEquals(numberHashFunctions, bf.nbHash);
		assertEquals(Hash.JENKINS_HASH, bf.hashType);
		assertNotNull(bf.bits);
		assertEquals(vectorSize * Byte.SIZE, bf.bits.size());
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAddNullKey() {
		bf.add((Key)null);
	}
	
	@Test
	public void testMembershipTestNullKey() {
		assertFalse(bf.membershipTest((Key)null));
	}
	
	@Test
	public void testAddKeyAndMembershipTest() throws UnsupportedEncodingException {
	    Key k1 = new StringKey("toto");
	    Key k2 = new StringKey("lulu");
	    Key k3 = new StringKey("mama");
	    Key k4 = new StringKey("graknyl");
	    Key k5 = new StringKey("xyzzy");
	    Key k6 = new StringKey("abcd");
	    
	    bf.add(k1);
	    assertFalse(bf.membershipTest(k4)); // no collision at this point

	    bf.add(k2);
	    assertTrue(bf.membershipTest(k4)); // this now collides

	    bf.add(k3);
	    assertTrue(bf.membershipTest(k4)); // this now collides

	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}

	@Test
	public void testAnd() throws UnsupportedEncodingException {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}", a.toString());

		BloomFilter b = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}", b.toString());
		
		a.and(b); // is like the intersection between two sets

		assertEquals("{6}", a.toString());
		assertEquals("{3, 4, 6}", b.toString());
	}

	@Test
	public void testOr() throws UnsupportedEncodingException {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}", a.toString());

		BloomFilter b = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}", b.toString());
		
		a.or(b); // is like the union between two sets

		assertEquals("{0, 3, 4, 6}", a.toString());
		assertEquals("{3, 4, 6}", b.toString());
	}

	@Test
	public void testXor() throws UnsupportedEncodingException {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}", a.toString());

		BloomFilter b = new BloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}", b.toString());
		
		a.xor(b); // is like the union less the intersection

		assertEquals("{0, 3, 4}", a.toString());
		assertEquals("{3, 4, 6}", b.toString());
	}

	@Test
	public void testNot() throws UnsupportedEncodingException {
		bf.add(new StringKey("toto"));
		assertEquals("{0, 6}", bf.toString());

		bf.not();

		assertEquals("{1, 2, 3, 4, 5}", bf.toString());	
	}

	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible1() {
		BloomFilter a = new BloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible2() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible3() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible1() {
		BloomFilter a = new BloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible2() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible3() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible1() {
		BloomFilter a = new BloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH);
		bf.xor(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible2() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH);
		bf.xor(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible3() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH);
		bf.xor(a);
	}
	
	@Test
	public void testToString() throws UnsupportedEncodingException {
		assertEquals("{}", bf.toString());
		bf.add(new StringKey("toto"));
		assertEquals("{0, 6}", bf.toString());
		bf.add(new StringKey("lulu"));
		assertEquals("{0, 3, 6, 7}", bf.toString());
		bf.add(new StringKey("mamma"));
		assertEquals("{0, 3, 6, 7}", bf.toString());
	}

	@Test
	public void testClone() {
		BloomFilter clone = (BloomFilter) bf.clone();

		assertNotNull(clone);
		assertEquals(bf.vectorSize, clone.vectorSize);
		assertEquals(bf.nbHash, clone.nbHash);
		assertEquals(bf.hashType, clone.hashType);
		assertNotNull(clone.bits);
		assertEquals(bf.bits.size(), clone.bits.size());
		assertEquals(bf.bits, clone.bits);
	}

}

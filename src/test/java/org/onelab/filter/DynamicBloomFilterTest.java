package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.onelab.filter.DynamicBloomFilter.LINE_SEPARATOR;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.hbase.util.Hash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DynamicBloomFilterTest {

	private DynamicBloomFilter bf = null;
	private static final int vectorSize = 8;
	private static final int numberHashFunctions = 2;
	private static final int maximumNumberOfKeysPerFilter = 2;
	
	@Before
	public void setUp() throws Exception {
		bf = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
	}

	@After
	public void tearDown() throws Exception {
		bf = null;
	}

	@Test
	public void testDynamicBloomFilterConstructor() {
		assertNotNull(bf);
		assertEquals(vectorSize, bf.vectorSize);
		assertEquals(numberHashFunctions, bf.nbHash);
		assertEquals(Hash.JENKINS_HASH, bf.hashType);
		assertEquals(maximumNumberOfKeysPerFilter, bf.nr);
		assertEquals(0, bf.currentNbRecord);
		assertEquals(1, bf.matrix.length);
		assertNotNull(bf.matrix[0]);
		assertTrue(bf.matrix[0] instanceof Filter);
		assertTrue(bf.matrix[0] instanceof BloomFilter);
		assertEquals(vectorSize, bf.matrix[0].vectorSize);
		assertEquals(numberHashFunctions, bf.matrix[0].nbHash);
		assertEquals(Hash.JENKINS_HASH, bf.matrix[0].hashType);
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
	public void testAddKeyAndMembershipTest2() throws UnsupportedEncodingException {
	    Key k1 = new StringKey("toto");
	    Key k2 = new StringKey("lulu");
	    Key k3 = new StringKey("mama");
	    Key k4 = new StringKey("graknyl");
	    Key k5 = new StringKey("xyzzy");
	    Key k6 = new StringKey("abcd");
	    
	    bf.add(k1);
	    assertEquals(1, bf.currentNbRecord);
	    assertEquals(1, bf.matrix.length);
	    assertTrue(bf.matrix[0].membershipTest(k1));
	    assertFalse(bf.membershipTest(k4)); // no collision at this point
	    assertFalse(bf.matrix[0].membershipTest(k4)); 

	    bf.add(k2);
	    assertEquals(2, bf.currentNbRecord);
	    assertEquals(1, bf.matrix.length);
	    assertTrue(bf.matrix[0].membershipTest(k2));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertTrue(bf.matrix[0].membershipTest(k4)); 

	    bf.add(k3);
	    assertEquals(1, bf.currentNbRecord);
	    assertEquals(2, bf.matrix.length);
	    assertFalse(bf.matrix[0].membershipTest(k3));
	    assertTrue(bf.matrix[1].membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertTrue(bf.matrix[0].membershipTest(k4)); 
	    assertFalse(bf.matrix[1].membershipTest(k4)); 

	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}

	@Test
	public void testAddListKeys() throws UnsupportedEncodingException {
	    Key k1 = new StringKey("toto");
	    Key k2 = new StringKey("lulu");
	    Key k3 = new StringKey("mama");
	    Key k4 = new StringKey("graknyl");
	    Key k5 = new StringKey("xyzzy");
	    Key k6 = new StringKey("abcd");
	    
		List<Key> keys = new ArrayList<Key>();
		keys.add(k1);
		keys.add(k2);
		keys.add(k3);
		
		bf.add(keys);
		
	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAddListKeysWithNull() throws UnsupportedEncodingException {
		bf.add((List<Key>)null);
	}
	
	@Test
	public void testAddCollectionKeys() throws UnsupportedEncodingException {
	    Key k1 = new StringKey("toto");
	    Key k2 = new StringKey("lulu");
	    Key k3 = new StringKey("mama");
	    Key k4 = new StringKey("graknyl");
	    Key k5 = new StringKey("xyzzy");
	    Key k6 = new StringKey("abcd");
	    
		Collection<Key> keys = new ArrayList<Key>();
		keys.add(k1);
		keys.add(k2);
		keys.add(k3);
		
		bf.add(keys);
		
	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAddCollectionKeysWithNull() throws UnsupportedEncodingException {
		bf.add((Collection<Key>)null);
	}
	
	@Test
	public void testAddArrayKeys() throws UnsupportedEncodingException {
	    Key k1 = new StringKey("toto");
	    Key k2 = new StringKey("lulu");
	    Key k3 = new StringKey("mama");
	    Key k4 = new StringKey("graknyl");
	    Key k5 = new StringKey("xyzzy");
	    Key k6 = new StringKey("abcd");
	    
		Collection<Key> keys = new ArrayList<Key>();
		keys.add(k1);
		keys.add(k2);
		keys.add(k3);

		bf.add(keys.toArray(new Key[]{}));

	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAddArrayKeysWithNull() throws UnsupportedEncodingException {
		bf.add((Key[])null);
	}
	
	@Test
	public void testAnd() throws UnsupportedEncodingException {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}" + LINE_SEPARATOR, a.toString());

		DynamicBloomFilter b = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
		
		a.and(b); // is like the intersection between two sets

		assertEquals("{6}" + LINE_SEPARATOR, a.toString());
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
	}

	@Test
	public void testOr() throws UnsupportedEncodingException {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}" + LINE_SEPARATOR, a.toString());

		DynamicBloomFilter b = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
		
		a.or(b); // is like the union between two sets

		assertEquals("{0, 3, 4, 6}" + LINE_SEPARATOR, a.toString());
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
	}

	@Test
	public void testXor() throws UnsupportedEncodingException {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		a.add(new StringKey("toto"));
		assertEquals("{0, 6}" + LINE_SEPARATOR, a.toString());

		DynamicBloomFilter b = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		b.add(new StringKey("lula"));
		b.add(new StringKey("to"));
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
		
		a.xor(b); // is like the union less the intersection

		assertEquals("{0, 3, 4}" + LINE_SEPARATOR, a.toString());
		assertEquals("{3, 4, 6}" + LINE_SEPARATOR, b.toString());
	}

	@Test
	public void testNot() throws UnsupportedEncodingException {
		bf.add(new StringKey("toto"));
		assertEquals("{0, 6}" + LINE_SEPARATOR, bf.toString());

		bf.not();

		assertEquals("{1, 2, 3, 4, 5}" + LINE_SEPARATOR, bf.toString());	
	}

	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible1() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible2() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible3() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH, maximumNumberOfKeysPerFilter);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible4() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, 10);
		bf.and(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testAndThrowsExceptionWhenFiltersAreIncompatible5() {
		bf.and(null);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible1() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible2() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testOrThrowsExceptionWhenFiltersAreIncompatible3() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH, maximumNumberOfKeysPerFilter);
		bf.or(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible1() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize + 1, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.xor(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible2() {
		DynamicBloomFilter a = new DynamicBloomFilter(vectorSize, numberHashFunctions + 1, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter);
		bf.xor(a);
	}
	
	@Test (expected=IllegalArgumentException.class)
	public void testXorThrowsExceptionWhenFiltersAreIncompatible3() {
		BloomFilter a = new BloomFilter(vectorSize, numberHashFunctions, Hash.MURMUR_HASH);
		bf.xor(a);
	}
	
	@Test
	public void testToString() throws UnsupportedEncodingException {
		assertEquals("{}" + LINE_SEPARATOR, bf.toString());
		bf.add(new StringKey("toto"));
		assertEquals("{0, 6}" + LINE_SEPARATOR, bf.toString());
		bf.add(new StringKey("lulu"));
		assertEquals("{0, 3, 6, 7}" + LINE_SEPARATOR, bf.toString());
		bf.add(new StringKey("mamma"));
		assertEquals("{0, 3, 6, 7}" + LINE_SEPARATOR + "{0}" + LINE_SEPARATOR, bf.toString());
	}

	@Test
	public void testClone() {
		DynamicBloomFilter clone = (DynamicBloomFilter) bf.clone();

		assertNotNull(clone);
		assertEquals(bf.vectorSize, clone.vectorSize);
		assertEquals(bf.nbHash, clone.nbHash);
		assertEquals(bf.hashType, clone.hashType);
		assertNotNull(clone.matrix[0].bits);
		assertEquals(bf.matrix[0].bits.size(), clone.matrix[0].bits.size());
		assertEquals(bf.matrix[0].bits, clone.matrix[0].bits);
	}

}

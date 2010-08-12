package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.onelab.filter.RotatingBloomFilter.LINE_SEPARATOR;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Hash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RotatingBloomFilterTest {

	private RotatingBloomFilter bf = null;
	private static final int vectorSize = 8;
	private static final int numberHashFunctions = 2;
	private static final int maximumNumberOfKeysPerFilter = 2;
	private static final int maximumNumberOfBloomFilters = 6;
	
	@Before
	public void setUp() throws Exception {
		bf = new RotatingBloomFilter(vectorSize, numberHashFunctions, Hash.JENKINS_HASH, maximumNumberOfKeysPerFilter, maximumNumberOfBloomFilters);
	}

	@After
	public void tearDown() throws Exception {
		bf = null;
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
	    assertEquals(1, bf.currentNumberOfKeys);
	    assertEquals(1, bf.filters.length);
	    assertTrue(bf.filters[0].membershipTest(k1));
	    assertFalse(bf.membershipTest(k4)); // no collision at this point
	    assertFalse(bf.filters[0].membershipTest(k4)); 

	    bf.add(k2);
	    assertEquals(2, bf.currentNumberOfKeys);
	    assertEquals(1, bf.filters.length);
	    assertTrue(bf.filters[0].membershipTest(k2));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertTrue(bf.filters[0].membershipTest(k4)); 

	    bf.add(k3);
	    assertEquals(1, bf.currentNumberOfKeys);
	    assertEquals(2, bf.filters.length);
	    assertFalse(bf.filters[0].membershipTest(k3));
	    assertTrue(bf.filters[1].membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertTrue(bf.filters[0].membershipTest(k4)); 
	    assertFalse(bf.filters[1].membershipTest(k4)); 

	    assertTrue(bf.membershipTest(k1));
	    assertTrue(bf.membershipTest(k2));
	    assertTrue(bf.membershipTest(k3));
	    assertTrue(bf.membershipTest(k4)); // this now collides
	    assertFalse(bf.membershipTest(k5));
	    assertFalse(bf.membershipTest(k6));
	}

	@Test
	public void testRotatingBloomFilter() {
		assertNotNull(bf);
		assertEquals(vectorSize, bf.vectorSize);
		assertEquals(numberHashFunctions, bf.nbHash);
		assertEquals(Hash.JENKINS_HASH, bf.hashType);
		assertEquals(maximumNumberOfKeysPerFilter, bf.maximumNumberOfKeysPerFilter);
		assertEquals(maximumNumberOfBloomFilters, bf.maximumNumberOfBloomFilters);
		assertEquals(0, bf.currentNumberOfKeys);
		assertEquals(1, bf.filters.length);
		assertNotNull(bf.filters[0]);
		assertTrue(bf.filters[0] instanceof Filter);
		assertTrue(bf.filters[0] instanceof BloomFilter);
		assertEquals(vectorSize, bf.filters[0].vectorSize);
		assertEquals(numberHashFunctions, bf.filters[0].nbHash);
		assertEquals(Hash.JENKINS_HASH, bf.filters[0].hashType);
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
	public void testGetVectorSize() {
		assertEquals(vectorSize, bf.getVectorSize());
	}
	
	@Test (expected=UnsupportedOperationException.class)
	public void testAnd() {
		bf.and(null);
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testOr() {
		bf.or(null);
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testXor() {
		bf.xor(null);
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testNot() {
		bf.not();
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testWrite() throws IOException {
		bf.write(null);
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testReadFields() throws IOException {
		bf.readFields(null);
	}

	@Test (expected=UnsupportedOperationException.class)
	public void testClone() {
		bf.clone();
	}

}

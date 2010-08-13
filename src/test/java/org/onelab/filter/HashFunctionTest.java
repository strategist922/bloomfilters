package org.onelab.filter;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.apache.hadoop.hbase.util.Hash;
import org.junit.Test;

public class HashFunctionTest {

	@Test
	public void testHashFunction() {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		
		assertNotNull(f);
		assertEquals(8, f.maxValue);
		assertEquals(2, f.nbHash);
		assertEquals(Hash.getInstance(Hash.JENKINS_HASH), f.hashFunction);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashFunctionWithNegativeValue1() {
		new HashFunction(-8, 2, Hash.JENKINS_HASH);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashFunctionWithNegativeValue2() {
		new HashFunction(8, -2, Hash.JENKINS_HASH);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashFunctionWithUnknownHashType() {
		new HashFunction(8, 2, Hash.INVALID_HASH);
	}

	@Test
	public void testClear() {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		f.clear();

		assertNotNull(f);
		assertEquals(8, f.maxValue);
		assertEquals(2, f.nbHash);
		assertEquals(Hash.getInstance(Hash.JENKINS_HASH), f.hashFunction);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashWithIllegalArgument1() {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		f.hash(null);
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashWithIllegalArgument2() {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		f.hash(new StubKeyNull(new String().getBytes()));
	}

	@Test (expected=IllegalArgumentException.class)
	public void testHashWithIllegalArgument3() {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		f.hash(new StubKeyZero(new String().getBytes()));
	}

	class StubKeyNull extends Key {
		public StubKeyNull(byte[] value) { super(value); }
		public byte[] getBytes() { return null; }
	}

	class StubKeyZero extends Key {
		public StubKeyZero(byte[] value) { super(value); }
		public byte[] getBytes() { return new byte[]{}; }
	}
	
	@Test
	public void testHash() throws UnsupportedEncodingException {
		HashFunction f = new HashFunction(8, 2, Hash.JENKINS_HASH);
		
		Key k1 = new StringKey("toto");
		Key k2 = new StringKey("lula");
		
		int[] h1 = f.hash(k1);
		int[] h2 = f.hash(k2);
		
		assertEquals(2, h1.length);
		assertEquals(6, h1[0]);
		assertEquals(0, h1[1]);

		assertEquals(2, h2.length);
		assertEquals(3, h2[0]);
		assertEquals(4, h2[1]);

		assertEquals(2, h1.length);
		assertEquals(6, h1[0]);
		assertEquals(0, h1[1]);
		
		f.clear();

		assertEquals(2, h1.length);
		assertEquals(6, h1[0]);
		assertEquals(0, h1[1]);
	}

}


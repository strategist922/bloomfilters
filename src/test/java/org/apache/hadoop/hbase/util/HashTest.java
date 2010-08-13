package org.apache.hadoop.hbase.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class HashTest {

	class MockHash extends Hash {
		public boolean called = false;
		public byte[] bytes;
		public int length;
		public int intval;

		public MockHash() {
			super();
		}
		
		@Override
		public int hash(byte[] bytes, int length, int initval) {
			called = true;
			this.bytes = bytes;
			this.length = length;
			this.intval = initval;

			return 1;
		}
	}
	
	@Test
	public void testParseHashType() {
		assertEquals(Hash.JENKINS_HASH, MockHash.parseHashType("jenkins"));
		assertEquals(Hash.MURMUR_HASH, MockHash.parseHashType("murmur"));
		assertEquals(Hash.JENKINS_HASH, MockHash.parseHashType("JenkinS"));
		assertEquals(Hash.MURMUR_HASH, MockHash.parseHashType("murMUR"));
		assertEquals(Hash.INVALID_HASH, MockHash.parseHashType("ciao"));
		assertEquals(Hash.INVALID_HASH, MockHash.parseHashType(null));
	}

	@Test
	public void testGetInstance() {
		assertEquals(JenkinsHash.getInstance(), MockHash.getInstance(Hash.JENKINS_HASH));
		assertEquals(MurmurHash.getInstance(), MockHash.getInstance(Hash.MURMUR_HASH));
		assertEquals(JenkinsHash.getInstance(), MockHash.getInstance(0));
		assertEquals(MurmurHash.getInstance(), MockHash.getInstance(1));
		assertNull(MockHash.getInstance(-1));
		assertNull(MockHash.getInstance(2));
	}

	@Test
	public void testHashByteArray() {
		MockHash hash = new MockHash();
		byte[] bytes = new String("toto").getBytes();
		
		assertEquals(1, hash.hash(bytes));
		assertEquals(bytes, hash.bytes);
		assertEquals(bytes.length, hash.length);
		assertEquals(-1, hash.intval);
	}

	@Test
	public void testHashByteArrayInt() {
		MockHash hash = new MockHash();
		byte[] bytes = new String("toto").getBytes();
		
		assertEquals(1, hash.hash(bytes, 2));
		assertEquals(bytes, hash.bytes);
		assertEquals(bytes.length, hash.length);
		assertEquals(2, hash.intval);
	}

	@Test
	public void testHashByteArrayIntInt() {
		MockHash hash = new MockHash();
		byte[] bytes = new String("toto").getBytes();
		
		assertEquals(1, hash.hash(bytes, 2, 3));
		assertEquals(bytes, hash.bytes);
		assertEquals(2, hash.length);
		assertEquals(3, hash.intval);
	}

}

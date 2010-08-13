package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

public class KeyTest {

	@Test
	public void testHashCode() {
		byte[] bytes = "toto".getBytes();
		Key key = new Key(bytes);

		assertEquals(1072693248, key.hashCode());
	}

	@Test
	public void testKeyConstructor() {
		byte[] bytes = "toto".getBytes();
		Key key = new Key(bytes);

		assertNotNull(key);
		assertEquals(bytes, key.getBytes());
		assertEquals(1.0, key.getWeight(), 0.01);
	}

	@Test
	public void testKeyConstructorWithWeight() {
		byte[] bytes = "toto".getBytes();
		Key key = new Key(bytes, 2.0);

		assertNotNull(key);
		assertEquals(bytes, key.getBytes());
		assertEquals(2.0, key.getWeight(), 0.01);
	}

	@Test (expected = IllegalArgumentException.class)
	public void testKeyConstructorWithNull() {
		new Key(null);
	}
	
	@Test
	public void testEquals() {
		byte[] b1 = "toto".getBytes();
		byte[] b2 = "toto".getBytes();
		byte[] b3 = "lula".getBytes();
		
		Key k1 = new Key(b1);
		Key k2 = new Key(b2);
		Key k3 = new Key(b3);
		Key k4 = new Key(b1, 2.0);
		Key k5 = new Key(b1, 1.0);
		
		assertEquals(k1, k2);
		assertEquals(k2, k1);
		assertEquals(k1, k5);
		assertEquals(k5, k1);
		assertFalse(k1.equals(k3));
		assertFalse(k3.equals(k1));
		assertFalse(k1.equals(k4));
		assertFalse(k4.equals(k1));
	}

	@Test
	public void testIncrementWeightDouble() {
		byte[] bytes = "toto".getBytes();
		Key key = new Key(bytes);

		assertNotNull(key);
		assertEquals(bytes, key.getBytes());
		assertEquals(1.0, key.getWeight(), 0.01);
		key.incrementWeight(2.0);
		assertEquals(3.0, key.getWeight(), 0.01);
	}

	@Test
	public void testIncrementWeight() {
		byte[] bytes = "toto".getBytes();
		Key key = new Key(bytes);

		assertNotNull(key);
		assertEquals(bytes, key.getBytes());
		assertEquals(1.0, key.getWeight(), 0.01);
		key.incrementWeight();
		assertEquals(2.0, key.getWeight(), 0.01);
	}

	@Test
	public void testEqualsObject() {
		byte[] bytes1 = "toto".getBytes();
		byte[] bytes2 = "toto".getBytes();
		byte[] bytes3 = "lulu".getBytes();

		Key key1 = new Key(bytes1);
		Key key2 = new Key(bytes2);
		Key key3 = new Key(bytes3);

		assertEquals(key1, key2);
		assertFalse(key1.equals(key3));
		assertFalse(key3.equals(key1));
		assertFalse(key2.equals(key3));
		assertFalse(key3.equals(key2));
	}

	@Test
	public void testCompareTo() {
		byte[] bytes1 = "toto".getBytes();
		byte[] bytes2 = "toto".getBytes();
		byte[] bytes3 = "lulu".getBytes();

		Key key1 = new Key(bytes1);
		Key key2 = new Key(bytes2);
		Key key3 = new Key(bytes3);

		assertEquals(0, key1.compareTo(key2));
		assertEquals(0, key2.compareTo(key1));
		assertEquals(-key3.compareTo(key1), key1.compareTo(key3));
	}

}

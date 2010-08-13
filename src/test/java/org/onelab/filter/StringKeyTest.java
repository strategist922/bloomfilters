package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

public class StringKeyTest {

	@Test
	public void testStringKeyConstructor() throws UnsupportedEncodingException {
		String str = "En Espa\u00f1ol";

		byte[] bytes1 = str.getBytes("UTF-16");
		byte[] bytes2 = str.getBytes("UTF-8");
		assertFalse(equals(bytes1, bytes2));
		
		StringKey key = new StringKey(str);

		assertNotNull(key);
		assertFalse(new String(bytes1).equals(new String(key.getBytes())));
		assertEquals(new String(bytes2), new String(key.getBytes()));
		assertEquals(1.0, key.getWeight(), 0.01);
	}
	
	@Test
	public void testStringKeyConstructorWithWeight() throws UnsupportedEncodingException {
		String str = "En Espa\u00f1ol";

		byte[] bytes1 = str.getBytes("UTF-16");
		byte[] bytes2 = str.getBytes("UTF-8");
		assertFalse(equals(bytes1, bytes2));
		
		StringKey key = new StringKey(str, 2.0);

		assertNotNull(key);
		assertFalse(new String(bytes1).equals(new String(key.getBytes())));
		assertEquals(new String(bytes2), new String(key.getBytes()));
		assertEquals(2.0, key.getWeight(), 0.01);
	}
	
	private boolean equals (byte[] b1, byte[] b2) {
		if ( ( b1 == null ) || ( b2 == null ) ) return false;
		if ( b1.length != b2.length ) return false;
		
		for ( int i = 0; i < b1.length; i++ ) {
			if ( b1[i] != b2[i] ) return false;
		}
		
		return true;
	}
	
}

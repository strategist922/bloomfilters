package org.onelab.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.UnsupportedEncodingException;

import org.junit.Test;


public class StringKeyTest {

	@Test
	public void testStringKeyConstructor() throws UnsupportedEncodingException {
		String str = "toto";
		byte[] bytes1 = str.getBytes();
		byte[] bytes2 = str.getBytes("UTF-8");
		StringKey key = new StringKey(str);

		assertNotNull(key);
		assertFalse(new String(bytes1).equals(new String(key.getBytes())));
		assertEquals(new String(bytes2), new String(key.getBytes()));
		assertEquals(1.0, key.getWeight(), 0.01);
	}
	
}

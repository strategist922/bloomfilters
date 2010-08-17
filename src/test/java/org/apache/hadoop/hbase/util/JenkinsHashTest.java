package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class JenkinsHashTest {

	@Test
	public void testGetInstance() {
		Hash hash = JenkinsHash.getInstance();
		assertNotNull(hash);
		assertEquals(hash, JenkinsHash.getInstance());
		assertSame(hash, JenkinsHash.getInstance());
	}
	
	@Test
	public void testJenkinsConstructor() {
		Hash hash = new JenkinsHash();
		assertNotNull(hash);
		assertFalse(hash.equals(JenkinsHash.getInstance()));
		assertNotSame(hash, JenkinsHash.getInstance());
	}
	
	@Test
	public void testHashByteArrayIntInt() {
		Hash hash = JenkinsHash.getInstance();
		
		byte[] bytes = "toto".getBytes();
		assertEquals(-515368923, hash.hash(bytes, bytes.length, -1));
		assertEquals(-24554862, hash.hash(bytes, bytes.length, 0));
		assertEquals(1554336639, hash.hash(bytes, bytes.length, 1));
		assertEquals(-644235423, hash.hash(bytes, bytes.length-2, 1));
	}

	@Test
	public void testHash1() throws IOException {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("lorem.txt");
		assertNotNull(in);
		// to enable rewind via reset()
		if (in.markSupported()) { 
			in.mark(1 * 1024 * 1024); 
		}

		byte[] bytes = new byte[512];
	    int value = 0;
	    JenkinsHash hash = new JenkinsHash();
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }
	    assertEquals(883342925, Math.abs(value));

	    // rewind
	    if (in.markSupported()) {
	    	in.reset();
	    } else {
	    	in = this.getClass().getClassLoader().getResourceAsStream("lorem.txt");
	    }
	    
	    value = 0;
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }	    
	    assertEquals(883342925, Math.abs(value));
	}
	
	@Test
	public void testHash2() throws IOException {
		InputStream in = this.getClass().getClassLoader().getResourceAsStream("lorem.txt");
		assertNotNull(in);
		// to enable rewind via reset()
		if (in.markSupported()) { 
			in.mark(1 * 1024 * 1024); 
		}

		byte[] bytes = new byte[512];
	    int value = 0;
	    Hash hash = JenkinsHash.getInstance();
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }
	    assertEquals(883342925, Math.abs(value));

	    // rewind
	    if (in.markSupported()) {
	    	in.reset();
	    } else {
	    	in = this.getClass().getClassLoader().getResourceAsStream("lorem.txt");
	    }
	    
	    value = 0;
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }	    
	    assertEquals(883342925, Math.abs(value));
	}
	
}

package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;

public class MurmurHashTest {

	@Test
	public void testGetInstance() {
		Hash hash = MurmurHash.getInstance();
		assertNotNull(hash);
		assertEquals(hash, MurmurHash.getInstance());
		assertSame(hash, MurmurHash.getInstance());
	}
	
	@Test
	public void testJenkinsConstructor() {
		Hash hash = new MurmurHash();
		assertNotNull(hash);
		assertFalse(hash.equals(MurmurHash.getInstance()));
		assertNotSame(hash, MurmurHash.getInstance());
	}
	
	@Test
	public void testHashByteArrayIntInt() {
		Hash hash = MurmurHash.getInstance();
		
		byte[] bytes = "toto".getBytes();
		assertEquals(-64559531, hash.hash(bytes, bytes.length, -1));
		assertEquals(2042495031, hash.hash(bytes, bytes.length, 0));
		assertEquals(1025330876, hash.hash(bytes, bytes.length, 1));
		assertEquals(1443387981, hash.hash(bytes, bytes.length-2, 1));
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
	    MurmurHash hash = new MurmurHash();
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }
	    assertEquals(641339114, Math.abs(value));

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
	    assertEquals(641339114, Math.abs(value));
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
	    Hash hash = MurmurHash.getInstance();
	    for (int length = in.read(bytes); length > 0 ; length = in.read(bytes)) {
	    	value = hash.hash(bytes, length, value);
	    }
	    assertEquals(641339114, Math.abs(value));

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
	    assertEquals(641339114, Math.abs(value));
	}
	
}

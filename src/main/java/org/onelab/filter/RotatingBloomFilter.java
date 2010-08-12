package org.onelab.filter;

/**
 * This is a small modification to org.onelab.filter.DynamicBloomFilter
 * DynamicBloomFilter is Copyright (c) 2005, European Commission project 
 * OneLab under contract 034819 (http://www.one-lab.org).
 * Licensed to the Apache Software Foundation (ASF).
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Hash;

public class RotatingBloomFilter extends Filter {

	protected BloomFilter[] filters;
	protected int currentNumberOfKeys; 
	protected int maximumNumberOfKeysPerFilter; 
	protected int maximumNumberOfBloomFilters;
	
	protected static final String LINE_SEPARATOR = new String(new byte[]{Character.LINE_SEPARATOR});

	/**
	 * Constructor.
	 * <p>
	 * Builds an empty Rotating Bloom filter.
	 * 
	 * @param vectorSize The number of bits in the vector.
	 * @param nbHash The number of hash function to consider.
	 * @param hashType type of the hashing function (see {@link Hash}).
	 * @param maximumNumberOfKeysPerFilter The threshold for the maximum number of keys to record in a rotating Bloom filter row.
	 * @param maximumNumberOfBloomFilters The threshold for the maximum number of rows.
	 */
	public RotatingBloomFilter(int vectorSize, int nbHash, int hashType, 
			int maximumNumberOfKeysPerFilter, int maximumNumberOfBloomFilters) {
		super(vectorSize, nbHash, hashType);

		this.maximumNumberOfKeysPerFilter = maximumNumberOfKeysPerFilter;
		this.maximumNumberOfBloomFilters = maximumNumberOfBloomFilters;
		this.currentNumberOfKeys = 0;

		filters = new BloomFilter[1];
		filters[0] = new BloomFilter(this.vectorSize, this.nbHash, this.hashType);
	}

	@Override
	public void add(Key key) {
		if (key == null) {
			throw new IllegalArgumentException("Key can not be null");
		}

		BloomFilter bf = getActiveStandardBF();

		if (bf == null) {
			addRow();
			bf = filters[filters.length - 1];
			currentNumberOfKeys = 0;
		}

		bf.add(key);

		currentNumberOfKeys++;
	}

	@Override
	public boolean membershipTest(Key key) {
		if (key == null) {
			return true;
		}

		for (int i = 0; i < filters.length; i++) {
			if (filters[i].membershipTest(key)) {
				return true;
			}
		}

		return false;
	}

	/**
	 * Adds a new row to <i>this</i> rotating Bloom filter.
	 */
	private void addRow() {
		BloomFilter[] tmp;
		if ( filters.length < maximumNumberOfBloomFilters ) { // add new rows
			tmp = new BloomFilter[filters.length + 1];

			for (int i = 0; i < filters.length; i++) {
				tmp[i] = (BloomFilter) filters[i].clone();
			}
		} else { // rotate, drop the oldest row (i.e. i=0)
			tmp = new BloomFilter[filters.length];

			for (int i = 0; i < filters.length - 1; i++) {
				tmp[i] = (BloomFilter) filters[i + 1].clone();
			}
		}
		tmp[tmp.length - 1] = new BloomFilter(vectorSize, nbHash, hashType);			

		filters = tmp;
	}

	/**
	 * Returns the active standard Bloom filter in <i>this</i> dynamic Bloom filter.
	 * 
	 * @return BloomFilter The active standard Bloom filter. <code>Null</code> otherwise.
	 */
	private BloomFilter getActiveStandardBF() {
		if (currentNumberOfKeys >= maximumNumberOfKeysPerFilter) {
			return null;
		}

		return filters[filters.length - 1];
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		for (int i = 0; i < filters.length; i++) {
			res.append(filters[i]);
			res.append(LINE_SEPARATOR);
		}
		return res.toString();
	}

	// We decided not to implement (and not to test!) things we do not need/use -- PC
	
	@Override
	public void and(Filter filter) {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public void not() {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public void or(Filter filter) {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public void xor(Filter filter) {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public Object clone() {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public void write(DataOutput out) throws IOException {
		throw new UnsupportedOperationException("Not implemented.");
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		throw new UnsupportedOperationException("Not implemented.");
	}
	
}

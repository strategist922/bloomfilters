package org.onelab.filter;

/**
 * This is a small modification to org.onelab.filter.RotatingBloomFilter
 * RotatingBloomFilter is Copyright (c) 2005, European Commission project 
 * OneLab under contract 034819 (http://www.one-lab.org).
 * Licensed to the Apache Software Foundation (ASF).
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Hash;

public class RotatingBloomFilter extends Filter {

	private int nr; // maximum number of keys in a rotating Bloom filter row.
	private int currentNbRecord; // number of keys recorded in the current standard active Bloom filter
	private BloomFilter[] filters; // array of Bloom filters
	private int nf; // maximum number of Bloom filters 

	/**
	 * Constructor.
	 * <p>
	 * Builds an empty Rotating Bloom filter.
	 * 
	 * @param vectorSize The number of bits in the vector.
	 * @param nbHash The number of hash function to consider.
	 * @param hashType type of the hashing function (see {@link Hash}).
	 * @param nr The threshold for the maximum number of keys to record in a rotating Bloom filter row.
	 * @param nf The threshold for the maximum number of rows.
	 */
	public RotatingBloomFilter(int vectorSize, int nbHash, int hashType, int nr, int nf) {
		super(vectorSize, nbHash, hashType);

		this.nr = nr;
		this.nf = nf;
		this.currentNbRecord = 0;

		filters = new BloomFilter[1];
		filters[0] = new BloomFilter(this.vectorSize, this.nbHash, this.hashType);
	}

	@Override
	public void add(Key key) {
		if (key == null) {
			throw new NullPointerException("Key can not be null");
		}

		BloomFilter bf = getActiveStandardBF();

		if (bf == null) {
			addRow();
			bf = filters[filters.length - 1];
			currentNbRecord = 0;
		}

		bf.add(key);

		currentNbRecord++;
	}

	@Override
	public void and(Filter filter) {
		if (filter == null || !(filter instanceof RotatingBloomFilter) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}

		RotatingBloomFilter dbf = (RotatingBloomFilter) filter;

		if (dbf.filters.length != this.filters.length || dbf.nr != this.nr) {
			throw new IllegalArgumentException("filters cannot be and-ed");
		}

		for (int i = 0; i < filters.length; i++) {
			filters[i].and(dbf.filters[i]);
		}
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

	@Override
	public void not() {
		for (int i = 0; i < filters.length; i++) {
			filters[i].not();
		}
	}

	@Override
	public void or(Filter filter) {
		if (filter == null || !(filter instanceof RotatingBloomFilter) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be or-ed");
		}

		RotatingBloomFilter dbf = (RotatingBloomFilter) filter;

		if (dbf.filters.length != this.filters.length || dbf.nr != this.nr) {
			throw new IllegalArgumentException("filters cannot be or-ed");
		}
		for (int i = 0; i < filters.length; i++) {
			filters[i].or(dbf.filters[i]);
		}
	}

	@Override
	public void xor(Filter filter) {
		if (filter == null || !(filter instanceof RotatingBloomFilter) || filter.vectorSize != this.vectorSize || filter.nbHash != this.nbHash) {
			throw new IllegalArgumentException("filters cannot be xor-ed");
		}
		RotatingBloomFilter dbf = (RotatingBloomFilter) filter;

		if (dbf.filters.length != this.filters.length || dbf.nr != this.nr) {
			throw new IllegalArgumentException("filters cannot be xor-ed");
		}

		for (int i = 0; i < filters.length; i++) {
			filters[i].xor(dbf.filters[i]);
		}
	}

	@Override
	public String toString() {
		StringBuilder res = new StringBuilder();

		for (int i = 0; i < filters.length; i++) {
			res.append(filters[i]);
			res.append(Character.LINE_SEPARATOR);
		}
		return res.toString();
	}

	@Override
	public Object clone() {
		RotatingBloomFilter rbf = new RotatingBloomFilter(vectorSize, nbHash, hashType, nr, nf);
		rbf.currentNbRecord = this.currentNbRecord;
		rbf.filters = new BloomFilter[this.filters.length];
		for (int i = 0; i < this.filters.length; i++) {
			rbf.filters[i] = (BloomFilter) this.filters[i].clone();
		}
		return rbf;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(nr);
		out.writeInt(currentNbRecord);
		out.writeInt(filters.length);
		for (int i = 0; i < filters.length; i++) {
			filters[i].write(out);
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		nr = in.readInt();
		currentNbRecord = in.readInt();
		int len = in.readInt();
		filters = new BloomFilter[len];
		for (int i = 0; i < filters.length; i++) {
			filters[i] = new BloomFilter();
			filters[i].readFields(in);
		}
	}

	/**
	 * Adds a new row to <i>this</i> rotating Bloom filter.
	 */
	private void addRow() {
		BloomFilter[] tmp;
		if ( filters.length <= nf ) { // add new rows
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
		if (currentNbRecord >= nr) {
			return null;
		}

		return filters[filters.length - 1];
	}

}

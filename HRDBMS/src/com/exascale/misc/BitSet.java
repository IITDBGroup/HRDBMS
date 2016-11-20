package com.exascale.misc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Iterator;

/**
 * <p>
 * This is an optimized version of Java's BitSet. In many cases, it can be used
 * as a drop-in replacement.
 * </p>
 *
 * <p>
 * It differs from the basic Java BitSet class in the following ways:
 * </p>
 * <ul>
 * <li>You can iterate over set bits using a simpler syntax
 * <code>for(int bs: myBitset)</code>.</li>
 * <li>You can compute the cardinality of an intersection and union without
 * writing it out or modifying your BitSets (see methods such as
 * andcardinality).</li>
 * <li>You can recover wasted memory with trim().</li>
 * <li>It does not implicitly expand: you have to explicitly call resize. This
 * helps to keep memory usage in check.</li>
 * <li>It supports memory-file mapping (see the ImmutableBitSet class).</li>
 * <li>It supports faster and more efficient serialization functions (serialize
 * and deserialize).</li>
 * </ul>
 *
 * @author Daniel Lemire
 * @since 0.8.0
 */
public class BitSet implements Cloneable, Iterable<Integer>, Externalizable, WordArray
{
	static final long serialVersionUID = 7997698588986878754L;

	long[] data;

	public BitSet()
	{
		this.data = new long[0];
	}

	/**
	 * Construct a bitset with the specified number of bits (initially all
	 * false). The number of bits is rounded up to the nearest multiple of 64.
	 *
	 * @param sizeInBits
	 *            the size in bits
	 */
	public BitSet(final int sizeInBits)
	{
		this.data = new long[(sizeInBits + 63) / 64];
	}

	/**
	 * Return a bitmap with the bit set to true at the given positions.
	 * 
	 * (This is a convenience method.)
	 *
	 * @param setBits
	 *            list of set bit positions
	 * @return the bitmap
	 */
	public static BitSet bitmapOf(final int... setBits)
	{
		int maxv = 0;
		for (final int k : setBits)
		{
			if (maxv < k)
			{
				maxv = k;
			}
		}
		final BitSet a = new BitSet(maxv + 1);
		for (final int k : setBits)
		{
			a.set(k);
		}
		return a;
	}

	/**
	 * Compute bitwise AND.
	 *
	 * @param bs
	 *            other bitset
	 */
	public void and(final WordArray bs)
	{
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			this.data[k] &= bs.getWord(k);
		}
	}

	/**
	 * Compute cardinality of bitwise AND.
	 * 
	 * The current bitmap is modified. Consider calling trim() to recover wasted
	 * memory afterward.
	 *
	 * @param bs
	 *            other bitset
	 * @return cardinality
	 */
	public int andcardinality(final WordArray bs)
	{
		int sum = 0;
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			sum += Long.bitCount(this.getWord(k) & bs.getWord(k));
		}
		return sum;
	}

	/**
	 * Compute bitwise AND NOT.
	 * 
	 * The current bitmap is modified. Consider calling trim() to recover wasted
	 * memory afterward.
	 *
	 * @param bs
	 *            other bitset
	 */
	public void andNot(final WordArray bs)
	{
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			this.data[k] &= ~bs.getWord(k);
		}
	}

	/**
	 * Compute cardinality of bitwise AND NOT.
	 *
	 * @param bs
	 *            other bitset
	 * @return cardinality
	 */
	public int andNotcardinality(final WordArray bs)
	{
		int sum = 0;
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			sum += Long.bitCount(this.getWord(k) & (~bs.getWord(k)));
		}
		return sum;
	}

	/**
	 * Compute the number of bits set to 1
	 *
	 * @return the number of bits
	 */
	public int cardinality()
	{
		int sum = 0;
		for (final long l : this.data)
		{
			sum += Long.bitCount(l);
		}
		return sum;
	}

	/**
	 * Reset all bits to false. This might be wasteful: a better approach is to
	 * create a new empty bitmap.
	 */
	public void clear()
	{
		Arrays.fill(this.data, 0);
	}

	/**
	 * Set the bit to false. See {@link #unset(int)}
	 * 
	 * @param index
	 *            location of the bit
	 */
	public void clear(final int index)
	{
		unset(index);
	}

	/**
	 * Set the bits in the range of indexes to false. This might throw an
	 * exception if size() is insufficient, consider calling resize().
	 * 
	 * @param start
	 *            location of the first bit to set to zero
	 * @param end
	 *            location of the last bit to set to zero (not included)
	 */
	public void clear(final int start, final int end)
	{
		if (start == end)
		{
			return;
		}
		final int firstword = start / 64;
		final int endword = (end - 1) / 64;
		if (firstword == endword)
		{
			this.data[firstword] &= ~((~0L << start) & (~0L >>> -end));
			return;
		}
		this.data[firstword] &= ~(~0L << start);
		for (int i = firstword + 1; i < endword; i++)
		{
			this.data[i] = 0;
		}
		this.data[endword] &= ~(~0L >>> -end);
	}

	@Override
	public BitSet clone()
	{
		BitSet b;
		try
		{
			b = (BitSet)super.clone();
			b.data = Arrays.copyOf(this.data, this.getNumberOfWords());
			return b;
		}
		catch (final CloneNotSupportedException e)
		{
			return null;
		}
	}

	/**
	 * Deserialize.
	 *
	 * @param in
	 *            the DataInput stream
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void deserialize(final DataInput in) throws IOException
	{
		final int length = (int)in.readLong();
		this.data = new long[length];
		for (int k = 0; k < length; ++k)
		{
			this.data[k] = in.readLong();
		}
	}

	/**
	 * Check whether a bitset contains a set bit.
	 *
	 * @return true if no set bit is found
	 */
	public boolean empty()
	{
		for (final long l : this.data)
		{
			if (l != 0)
			{
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean equals(final Object o)
	{
		if (o instanceof WordArray)
		{
			final WordArray bs = (WordArray)o;
			for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
			{
				if (this.getWord(k) != bs.getWord(k))
				{
					return false;
				}
			}
			final WordArray longer = bs.getNumberOfWords() < this.getNumberOfWords() ? this : bs;
			for (int k = Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); k < Math.max(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
			{
				if (longer.getWord(k) != 0)
				{
					return false;
				}
			}
			return true;
		}
		return false;
	}

	/**
	 * Flip the bit. This might throw an exception if size() is insufficient,
	 * consider calling resize().
	 *
	 * @param i
	 *            index of the bit
	 */
	public void flip(final int i)
	{
		this.data[i / 64] ^= (1l << (i % 64));
	}

	/**
	 * Flip the bits in the range of indexes. This might throw an exception if
	 * size() is insufficient, consider calling resize().
	 * 
	 * @param start
	 *            location of the first bit
	 * @param end
	 *            location of the last bit (not included)
	 */
	public void flip(final int start, final int end)
	{
		if (start == end)
		{
			return;
		}
		final int firstword = start / 64;
		final int endword = (end - 1) / 64;
		this.data[firstword] ^= ~(~0L << start);
		for (int i = firstword; i < endword; i++)
		{
			this.data[i] = ~this.data[i];
		}
		this.data[endword] ^= ~0L >>> -end;
	}

	/**
	 * Get the value of the bit. This might throw an exception if size() is
	 * insufficient, consider calling resize().
	 * 
	 * @param i
	 *            index
	 * @return value of the bit
	 */
	public boolean get(final int i)
	{
		return (this.data[i / 64] & (1l << (i % 64))) != 0;
	}

	@Override
	public int getNumberOfWords()
	{
		return data.length;
	}

	@Override
	public long getWord(final int index)
	{
		return this.data[index];
	}

	@Override
	public int hashCode()
	{
		final int b = 31;
		long hash = 0;
		for (int k = 0; k < data.length; ++k)
		{
			final long aData = this.getWord(k);
			hash = hash * b + aData;
		}
		return (int)hash;
	}

	/**
	 * Checks whether two bitsets intersect.
	 *
	 * @param bs
	 *            other bitset
	 * @return true if they have a non-empty intersection (result of AND)
	 */
	public boolean intersects(final WordArray bs)
	{
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			if ((this.getWord(k) & bs.getWord(k)) != 0)
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * Iterate over the set bits
	 *
	 * @return an iterator
	 */
	public IntIterator intIterator()
	{
		return new IntIterator() {
			private int i = BitSet.this.nextSetBit(0);

			private int j;

			@Override
			public boolean hasNext()
			{
				return this.i >= 0;
			}

			@Override
			public int next()
			{
				this.j = this.i;
				this.i = BitSet.this.nextSetBit(this.i + 1);
				return this.j;
			}

		};
	}

	@Override
	public Iterator<Integer> iterator()
	{
		return new Iterator<Integer>() {
			private int i = BitSet.this.nextSetBit(0);

			private int j;

			@Override
			public boolean hasNext()
			{
				return this.i >= 0;
			}

			@Override
			public Integer next()
			{
				this.j = this.i;
				this.i = BitSet.this.nextSetBit(this.i + 1);
				return this.j;
			}

			@Override
			public void remove()
			{
				BitSet.this.unset(this.j);
			}
		};
	}

	/**
	 * Usage: for(int i=bs.nextSetBit(0); i&gt;=0; i=bs.nextSetBit(i+1)) {
	 * operate on index i here }
	 *
	 * @param i
	 *            current set bit
	 * @return next set bit or -1
	 */
	public int nextSetBit(final int i)
	{
		int x = i / 64;
		if (x >= this.getNumberOfWords())
		{
			return -1;
		}
		long w = this.data[x];
		w >>>= i;
		if (w != 0)
		{
			return i + Long.numberOfTrailingZeros(w);
		}
		++x;
		for (; x < this.getNumberOfWords(); ++x)
		{
			if (this.data[x] != 0)
			{
				return x * 64 + Long.numberOfTrailingZeros(this.data[x]);
			}
		}
		return -1;
	}

	/**
	 * Usage: for(int i=bs.nextUnsetBit(0); i&gt;=0; i=bs.nextUnsetBit(i+1)) {
	 * operate on index i here }
	 *
	 * @param i
	 *            current unset bit
	 * @return next unset bit or -1
	 */
	public int nextUnsetBit(final int i)
	{
		int x = i / 64;
		if (x >= this.getNumberOfWords())
		{
			return -1;
		}
		long w = ~this.data[x];
		w >>>= i;
		if (w != 0)
		{
			return i + Long.numberOfTrailingZeros(w);
		}
		++x;
		for (; x < this.getNumberOfWords(); ++x)
		{
			if (this.data[x] != ~0)
			{
				return x * 64 + Long.numberOfTrailingZeros(~this.data[x]);
			}
		}
		return -1;
	}

	/**
	 * Compute bitwise OR.
	 * 
	 * The current bitmap is modified. Consider calling trim() to recover wasted
	 * memory afterward.
	 *
	 * @param bs
	 *            other bitset
	 */
	public void or(final WordArray bs)
	{
		if (this.getNumberOfWords() < bs.getNumberOfWords())
		{
			this.resize(bs.getNumberOfWords() * 64);
		}
		for (int k = 0; k < this.getNumberOfWords(); ++k)
		{
			this.data[k] |= bs.getWord(k);
		}
	}

	/**
	 * Compute cardinality of bitwise OR.
	 * 
	 * BitSets are not modified.
	 *
	 * @param bs
	 *            other bitset
	 * @return cardinality
	 */
	public int orcardinality(final WordArray bs)
	{
		int sum = 0;
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			sum += Long.bitCount(this.getWord(k) | bs.getWord(k));
		}
		final WordArray longer = bs.getNumberOfWords() < this.getNumberOfWords() ? this : bs;
		for (int k = Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); k < Math.max(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			sum += Long.bitCount(longer.getWord(k));
		}
		return sum;
	}

	@Override
	public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException
	{
		deserialize(in);
	}

	/**
	 * Resize the bitset
	 *
	 * @param sizeInBits
	 *            new number of bits
	 */
	public void resize(final int sizeInBits)
	{
		this.data = Arrays.copyOf(this.data, (sizeInBits + 63) / 64);
	}

	/**
	 * Serialize.
	 *
	 * The current bitmap is not modified.
	 *
	 * @param out
	 *            the DataOutput stream
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public void serialize(final DataOutput out) throws IOException
	{
		out.writeLong(this.getNumberOfWords());
		for (final long w : this.data)
		{
			out.writeLong(w);
		}
	}

	/**
	 * Set to true. This might throw an exception if size() is insufficient,
	 * consider calling resize().
	 *
	 * @param i
	 *            index of the bit
	 */
	public void set(final int i)
	{
		this.data[i / 64] |= (1l << (i % 64));
	}

	/**
	 * Set to some value. This might throw an exception if size() is
	 * insufficient, consider calling resize().
	 *
	 * @param i
	 *            index
	 * @param b
	 *            value of the bit
	 */
	public void set(final int i, final boolean b)
	{
		if (b)
		{
			set(i);
		}
		else
		{
			unset(i);
		}
	}

	/**
	 * Set the bits in the range of indexes true. This might throw an exception
	 * if size() is insufficient, consider calling resize().
	 * 
	 * @param start
	 *            location of the first bit
	 * @param end
	 *            location of the last bit (not included)
	 */
	public void set(final int start, final int end)
	{
		if (start == end)
		{
			return;
		}
		final int firstword = start / 64;
		final int endword = (end - 1) / 64;
		if (firstword == endword)
		{
			this.data[firstword] |= (~0L << start) & (~0L >>> -end);
			return;
		}
		this.data[firstword] |= ~0L << start;
		for (int i = firstword + 1; i < endword; i++)
		{
			this.data[i] = ~0;
		}
		this.data[endword] |= ~0L >>> -end;
	}

	/**
	 * Set the bits in the range of indexes to the specified Boolean value. This
	 * might throw an exception if size() is insufficient, consider calling
	 * resize().
	 * 
	 * @param start
	 *            location of the first bit
	 * @param end
	 *            location of the last bit (not included)
	 * @param v
	 *            Boolean value
	 */
	public void set(final int start, final int end, final boolean v)
	{
		if (v)
		{
			set(start, end);
		}
		else
		{
			clear(start, end);
		}
	}

	/**
	 * Query the size
	 *
	 * @return the size in bits.
	 */
	public int size()
	{
		return this.getNumberOfWords() * 64;
	}

	@Override
	public String toString()
	{
		final StringBuilder answer = new StringBuilder();
		final IntIterator i = this.intIterator();
		answer.append("{");
		if (i.hasNext())
		{
			answer.append(i.next());
		}
		while (i.hasNext())
		{
			answer.append(",");
			answer.append(i.next());
		}
		answer.append("}");
		return answer.toString();
	}

	/**
	 * Recovers wasted memory
	 */
	public void trim()
	{
		for (int k = this.getNumberOfWords() - 1; k >= 0; --k)
		{
			if (this.getWord(k) != 0)
			{
				if (k + 1 < this.getNumberOfWords())
				{
					this.data = Arrays.copyOf(this.data, k + 1);
				}
				return;
			}
		}
		this.data = new long[0];
	}

	/**
	 * Set to false
	 *
	 * @param i
	 *            index of the bit
	 */
	public void unset(final int i)
	{
		this.data[i / 64] &= ~(1l << (i % 64));
	}

	/**
	 * Iterate over the unset bits
	 *
	 * @return an iterator
	 */
	public IntIterator unsetIntIterator()
	{
		return new IntIterator() {
			private int i = BitSet.this.nextUnsetBit(0);

			private int j;

			@Override
			public boolean hasNext()
			{
				return this.i >= 0;
			}

			@Override
			public int next()
			{
				this.j = this.i;
				this.i = BitSet.this.nextUnsetBit(this.i + 1);
				return this.j;
			}
		};
	}

	@Override
	public void writeExternal(final ObjectOutput out) throws IOException
	{
		serialize(out);
	}

	/**
	 * Compute bitwise XOR.
	 * 
	 * The current bitmap is modified. Consider calling trim() to recover wasted
	 * memory afterward.
	 *
	 * @param bs
	 *            other bitset
	 */
	public void xor(final WordArray bs)
	{
		if (this.getNumberOfWords() < bs.getNumberOfWords())
		{
			this.resize(bs.getNumberOfWords() * 64);
		}
		for (int k = 0; k < this.getNumberOfWords(); ++k)
		{
			this.data[k] ^= bs.getWord(k);
		}
	}

	/**
	 * Compute cardinality of bitwise XOR.
	 * 
	 * BitSets are not modified.
	 *
	 * @param bs
	 *            other bitset
	 * @return cardinality
	 */
	public int xorcardinality(final WordArray bs)
	{
		int sum = 0;
		for (int k = 0; k < Math.min(this.getNumberOfWords(), bs.getNumberOfWords()); ++k)
		{
			sum += Long.bitCount(this.getWord(k) ^ bs.getWord(k));
		}
		final WordArray longer = bs.getNumberOfWords() < this.getNumberOfWords() ? this : bs;

		final int start = Math.min(this.getNumberOfWords(), bs.getNumberOfWords());
		final int end = Math.max(this.getNumberOfWords(), bs.getNumberOfWords());
		for (int k = start; k < end; ++k)
		{
			sum += Long.bitCount(longer.getWord(k));
		}

		return sum;
	}

}
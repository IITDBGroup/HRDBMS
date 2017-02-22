package com.exascale.misc;

public final class BitCounter implements BitmapStorage
{

	private int oneBits;

	/**
	 * Virtually add literal words directly to the bitmap
	 *
	 * @param newData
	 *            the word
	 */
	@Override
	public void addLiteralWord(final long newData)
	{
		this.oneBits += Long.bitCount(newData);
	}

	/**
	 * virtually add many zeroes or ones.
	 *
	 * @param v
	 *            zeros or ones
	 * @param number
	 *            how many to words add
	 */
	@Override
	public void addStreamOfEmptyWords(final boolean v, final long number)
	{
		if (v)
		{
			this.oneBits += number * CompressedBitSet.WORD_IN_BITS;
		}
	}

	/**
	 * virtually add several literal words.
	 *
	 * @param buffer
	 *            the buffer wrapping the literal words
	 * @param start
	 *            the starting point in the array
	 * @param number
	 *            the number of literal words to add
	 */
	@Override
	public void addStreamOfLiteralWords(final Buffer buffer, final int start, final int number)
	{
		for (int i = start; i < start + number; i++)
		{
			addLiteralWord(buffer.getWord(i));
		}
	}

	/**
	 * virtually add several negated literal words.
	 *
	 * @param buffer
	 *            the buffer wrapping the literal words
	 * @param start
	 *            the starting point in the array
	 * @param number
	 *            the number of literal words to add
	 */
	@Override
	public void addStreamOfNegatedLiteralWords(final Buffer buffer, final int start, final int number)
	{
		for (int i = start; i < start + number; i++)
		{
			addLiteralWord(~buffer.getWord(i));
		}
	}

	/**
	 * Virtually add words directly to the bitmap
	 *
	 * @param newData
	 *            the word
	 */
	@Override
	public void addWord(final long newData)
	{
		this.oneBits += Long.bitCount(newData);
	}

	@Override
	public void clear()
	{
		this.oneBits = 0;
	}

	/**
	 * As you act on this class, it records the number of set (true) bits.
	 *
	 * @return number of set bits
	 */
	public int getCount()
	{
		return this.oneBits;
	}

	/**
	 * should directly set the sizeInBits field, but is effectively ignored in
	 * this class.
	 *
	 * @param bits
	 *            number of bits
	 */
	// @Override : causes problems with Java 1.5
	@Override
	public void setSizeInBitsWithinLastWord(final int bits)
	{
		// no action
	}
}
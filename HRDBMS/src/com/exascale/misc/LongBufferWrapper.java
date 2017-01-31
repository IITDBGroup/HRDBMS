package com.exascale.misc;

import java.nio.LongBuffer;

/**
 * java.nio.LongBuffer wrapper. Users should not be concerned by this class.
 *
 * @author Gregory Ssi-Yan-Kai
 */
final class LongBufferWrapper implements Buffer, Cloneable
{

	/**
	 * The actual size in words.
	 */
	private int actualSizeInWords = 1;

	/**
	 * The buffer
	 */
	private LongBuffer buffer;

	public LongBufferWrapper(final LongBuffer buffer)
	{
		this.buffer = buffer;
	}

	public LongBufferWrapper(final LongBuffer slice, final int sizeInWords)
	{
		this.buffer = slice;
		this.actualSizeInWords = sizeInWords;
	}

	@Override
	public void andLastWord(final long mask)
	{
		andWord(this.actualSizeInWords - 1, mask);
	}

	@Override
	public void andWord(final int position, final long mask)
	{
		setWord(position, getWord(position) & mask);
	}

	@Override
	public void clear()
	{
		this.actualSizeInWords = 1;
		setWord(0, 0);
	}

	@Override
	public LongBufferWrapper clone()
	{
		return new LongBufferWrapper(this.buffer, this.actualSizeInWords);
	}

	@Override
	public void collapse(final int position, final int length)
	{
		for (int i = 0; i < this.actualSizeInWords - position - length; ++i)
		{
			setWord(position + i, getWord(position + length + i));
		}
		for (int i = 0; i < length; ++i)
		{
			removeLastWord();
		}
	}

	@Override
	public void ensureCapacity(final int capacity)
	{
		if (capacity > buffer.capacity())
		{
			throw new RuntimeException("Cannot increase buffer capacity. Current capacity: " + buffer.capacity() + ". New capacity: " + capacity);
		}
	}

	@Override
	public void expand(final int position, final int length)
	{
		for (int i = this.actualSizeInWords - position - 1; i >= 0; --i)
		{
			setWord(position + length + i, getWord(position + i));
		}
		this.actualSizeInWords += length;
	}

	@Override
	public long getLastWord()
	{
		return getWord(this.actualSizeInWords - 1);
	}

	@Override
	public long getWord(final int position)
	{
		return this.buffer.get(position);
	}

	@Override
	public void negateWord(final int position)
	{
		setWord(position, ~getWord(position));
	}

	@Override
	public void negative_push_back(final Buffer buffer, final int start, final int number)
	{
		for (int i = 0; i < number; ++i)
		{
			push_back(~buffer.getWord(start + i));
		}
	}

	@Override
	public void orLastWord(final long mask)
	{
		orWord(this.actualSizeInWords - 1, mask);
	}

	@Override
	public void orWord(final int position, final long mask)
	{
		setWord(position, getWord(position) | mask);
	}

	@Override
	public void push_back(final Buffer buffer, final int start, final int number)
	{
		for (int i = 0; i < number; ++i)
		{
			push_back(buffer.getWord(start + i));
		}
	}

	@Override
	public void push_back(final long word)
	{
		setWord(this.actualSizeInWords++, word);
	}

	@Override
	public void removeLastWord()
	{
		setWord(--this.actualSizeInWords, 0l);
	}

	@Override
	public void setLastWord(final long word)
	{
		setWord(this.actualSizeInWords - 1, word);
	}

	@Override
	public void setWord(final int position, final long word)
	{
		this.buffer.put(position, word);
	}

	@Override
	public int sizeInWords()
	{
		return this.actualSizeInWords;
	}

	@Override
	public void swap(final Buffer other)
	{
		if (other instanceof LongBufferWrapper)
		{// optimized version
			final LongBufferWrapper o = (LongBufferWrapper)other;
			final LongBuffer tmp = this.buffer;
			final int tmp2 = this.actualSizeInWords;
			this.actualSizeInWords = o.actualSizeInWords;
			this.buffer = o.buffer;
			o.actualSizeInWords = tmp2;
			o.buffer = tmp;
		}
		else
		{
			other.swap(this);
		}
	}

	@Override
	public void trim()
	{
	}

}
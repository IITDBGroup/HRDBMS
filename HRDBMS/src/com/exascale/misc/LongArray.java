package com.exascale.misc;

import java.util.Arrays;

/**
 * Long array wrapper. Users should not be concerned by this class.
 *
 * @author Gregory Ssi-Yan-Kai
 */
final class LongArray implements Buffer, Cloneable
{

	/**
	 * The Constant DEFAULT_BUFFER_SIZE: default memory allocation when the
	 * object is constructed.
	 */
	private static final int DEFAULT_BUFFER_SIZE = 4;

	/**
	 * The actual size in words.
	 */
	private int actualSizeInWords = 1;

	/**
	 * The buffer (array of 64-bit words)
	 */
	private long buffer[] = null;

	/**
	 * Creates a buffer with default size
	 */
	public LongArray()
	{
		this(DEFAULT_BUFFER_SIZE);
	}

	/**
	 * Creates a buffer with explicit size
	 * 
	 * @param bufferSize
	 */
	public LongArray(int bufferSize)
	{
		if (bufferSize < 1)
		{
			bufferSize = 1;
		}
		this.buffer = new long[bufferSize];
	}

	@Override
	public void andLastWord(final long mask)
	{
		andWord(this.actualSizeInWords - 1, mask);
	}

	@Override
	public void andWord(final int position, final long mask)
	{
		this.buffer[position] &= mask;
	}

	@Override
	public void clear()
	{
		this.actualSizeInWords = 1;
		this.buffer[0] = 0;
	}

	@Override
	public LongArray clone()
	{
		LongArray clone = null;
		try
		{
			clone = (LongArray)super.clone();
			clone.buffer = this.buffer.clone();
			clone.actualSizeInWords = this.actualSizeInWords;
		}
		catch (final CloneNotSupportedException e)
		{
			e.printStackTrace(); // cannot happen
		}
		return clone;
	}

	@Override
	public void collapse(final int position, final int length)
	{
		System.arraycopy(this.buffer, position + length, this.buffer, position, this.actualSizeInWords - position - length);
		for (int i = 0; i < length; ++i)
		{
			removeLastWord();
		}
	}

	@Override
	public void ensureCapacity(final int capacity)
	{
		resizeBuffer(capacity - this.actualSizeInWords);
	}

	@Override
	public void expand(final int position, final int length)
	{
		resizeBuffer(length);
		System.arraycopy(this.buffer, position, this.buffer, position + length, this.actualSizeInWords - position);
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
		return this.buffer[position];
	}

	@Override
	public void negateWord(final int position)
	{
		this.buffer[position] = ~this.buffer[position];
	}

	@Override
	public void negative_push_back(final Buffer buffer, final int start, final int number)
	{
		resizeBuffer(number);
		for (int i = 0; i < number; ++i)
		{
			this.buffer[this.actualSizeInWords + i] = ~buffer.getWord(start + i);
		}
		this.actualSizeInWords += number;
	}

	@Override
	public void orLastWord(final long mask)
	{
		orWord(this.actualSizeInWords - 1, mask);
	}

	@Override
	public void orWord(final int position, final long mask)
	{
		this.buffer[position] |= mask;
	}

	@Override
	public void push_back(final Buffer buffer, final int start, final int number)
	{
		resizeBuffer(number);
		if (buffer instanceof LongArray)
		{
			final long[] data = ((LongArray)buffer).buffer;
			System.arraycopy(data, start, this.buffer, this.actualSizeInWords, number);
		}
		else
		{
			for (int i = 0; i < number; ++i)
			{
				this.buffer[this.actualSizeInWords + i] = buffer.getWord(start + i);
			}
		}
		this.actualSizeInWords += number;
	}

	@Override
	public void push_back(final long word)
	{
		resizeBuffer(1);
		this.buffer[this.actualSizeInWords++] = word;
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
		this.buffer[position] = word;
	}

	@Override
	public int sizeInWords()
	{
		return this.actualSizeInWords;
	}

	@Override
	public void swap(final Buffer other)
	{
		if (other instanceof LongArray)
		{
			final long[] tmp = this.buffer;
			this.buffer = ((LongArray)other).buffer;
			((LongArray)other).buffer = tmp;

			final int tmp2 = this.actualSizeInWords;
			this.actualSizeInWords = ((LongArray)other).actualSizeInWords;
			((LongArray)other).actualSizeInWords = tmp2;
		}
		else
		{
			final long[] tmp = new long[other.sizeInWords()];
			for (int i = 0; i < other.sizeInWords(); ++i)
			{
				tmp[i] = other.getWord(i);
			}
			final int tmp2 = other.sizeInWords();

			other.clear();
			other.removeLastWord();
			other.push_back(this, 0, this.sizeInWords());

			this.buffer = tmp;
			this.actualSizeInWords = tmp2;
		}
	}

	@Override
	public void trim()
	{
		this.buffer = Arrays.copyOf(this.buffer, this.actualSizeInWords);
	}

	/**
	 * Returns the resulting buffer size in words given the number of words to
	 * add.
	 * 
	 * @param number
	 *            the number of words to add
	 */
	private int newSizeInWords(final int number)
	{
		int size = this.actualSizeInWords + number;
		if (size >= this.buffer.length)
		{
			if (size < 32768)
			{
				size = size * 2;
			}
			else if (size * 3 / 2 < size)
			{
				size = Integer.MAX_VALUE;
			}
			else
			{
				size = size * 3 / 2;
			}
		}
		return size;
	}

	/**
	 * Resizes the buffer if the number of words to add exceeds the buffer
	 * capacity.
	 * 
	 * @param number
	 *            the number of words to add
	 */
	private void resizeBuffer(final int number)
	{
		final int size = newSizeInWords(number);
		if (size >= this.buffer.length)
		{
			final long oldBuffer[] = this.buffer;
			this.buffer = new long[size];
			System.arraycopy(oldBuffer, 0, this.buffer, 0, oldBuffer.length);
		}
	}

}
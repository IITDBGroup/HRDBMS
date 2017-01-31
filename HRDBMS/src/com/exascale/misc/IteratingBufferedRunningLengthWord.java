package com.exascale.misc;

public final class IteratingBufferedRunningLengthWord implements IteratingRLW, Cloneable
{
	private BufferedRunningLengthWord brlw;

	private final Buffer buffer;

	private int literalWordStartPosition;
	private EWAHIterator iterator;

	/**
	 * Instantiates a new iterating buffered running length word.
	 *
	 * @param bitmap
	 *            over which we want to iterate
	 */
	public IteratingBufferedRunningLengthWord(final CompressedBitSet bitmap)
	{
		this(EWAHIterator.getEWAHIterator(bitmap));
	}

	/**
	 * Instantiates a new iterating buffered running length word.
	 *
	 * @param iterator
	 *            iterator
	 */
	public IteratingBufferedRunningLengthWord(final EWAHIterator iterator)
	{
		this.iterator = iterator;
		this.brlw = new BufferedRunningLengthWord(this.iterator.next());
		this.literalWordStartPosition = this.iterator.literalWords() + this.brlw.literalWordOffset;
		this.buffer = this.iterator.buffer();
	}

	/**
	 * For internal use. (One could use the non-static discharge method instead,
	 * but we expect them to be slower.)
	 *
	 * @param initialWord
	 *            the initial word
	 * @param iterator
	 *            the iterator
	 * @param container
	 *            the container
	 */
	private static void discharge(final BufferedRunningLengthWord initialWord, final EWAHIterator iterator, final BitmapStorage container)
	{
		BufferedRunningLengthWord runningLengthWord = initialWord;
		for (;;)
		{
			final long runningLength = runningLengthWord.getRunningLength();
			container.addStreamOfEmptyWords(runningLengthWord.getRunningBit(), runningLength);
			container.addStreamOfLiteralWords(iterator.buffer(), iterator.literalWords() + runningLengthWord.literalWordOffset, runningLengthWord.getNumberOfLiteralWords());
			if (!iterator.hasNext())
			{
				break;
			}
			runningLengthWord = new BufferedRunningLengthWord(iterator.next());
		}
	}

	@Override
	public IteratingBufferedRunningLengthWord clone() throws CloneNotSupportedException
	{
		final IteratingBufferedRunningLengthWord answer = (IteratingBufferedRunningLengthWord)super.clone();
		answer.brlw = this.brlw.clone();
		answer.iterator = this.iterator.clone();
		return answer;
	}

	/**
	 * Discard first words, iterating to the next running length word if needed.
	 *
	 * @param x
	 *            the number of words to be discarded
	 */
	@Override
	public void discardFirstWords(long x)
	{
		while (x > 0)
		{
			if (this.brlw.runningLength > x)
			{
				this.brlw.runningLength -= x;
				return;
			}
			x -= this.brlw.runningLength;
			this.brlw.runningLength = 0;
			final long toDiscard = x > this.brlw.numberOfLiteralWords ? this.brlw.numberOfLiteralWords : x;

			this.literalWordStartPosition += toDiscard;
			this.brlw.numberOfLiteralWords -= toDiscard;
			x -= toDiscard;
			if ((x > 0) || (this.brlw.size() == 0))
			{
				if (!this.iterator.hasNext())
				{
					break;
				}
				this.brlw.reset(this.iterator.next());
				this.literalWordStartPosition = this.iterator.literalWords();
			}
		}
	}

	@Override
	public void discardLiteralWords(final long x)
	{
		this.literalWordStartPosition += x;
		this.brlw.numberOfLiteralWords -= x;
		if (this.brlw.numberOfLiteralWords == 0)
		{
			if (!this.iterator.hasNext())
			{
				return;
			}
			this.brlw.reset(this.iterator.next());
			this.literalWordStartPosition = this.iterator.literalWords();
		}
	}

	@Override
	public void discardRunningWords()
	{
		this.brlw.runningLength = 0;
		if (this.brlw.getNumberOfLiteralWords() == 0)
		{
			this.next();
		}
	}

	/**
	 * Write out the remaining words
	 *
	 * @param container
	 *            target for writes
	 */
	public void discharge(final BitmapStorage container)
	{
		this.brlw.literalWordOffset = this.literalWordStartPosition - this.iterator.literalWords();
		discharge(this.brlw, this.iterator, container);
	}

	/**
	 * Write out up to max words, returns how many were written
	 *
	 * @param container
	 *            target for writes
	 * @param max
	 *            maximal number of writes
	 * @return how many written
	 */
	public long discharge(final BitmapStorage container, final long max)
	{
		long index = 0;
		while (true)
		{
			if (index + getRunningLength() > max)
			{
				final int offset = (int)(max - index);
				container.addStreamOfEmptyWords(getRunningBit(), offset);
				this.brlw.runningLength -= offset;
				return max;
			}
			container.addStreamOfEmptyWords(getRunningBit(), getRunningLength());
			index += getRunningLength();
			if (getNumberOfLiteralWords() + index > max)
			{
				final int offset = (int)(max - index);
				writeLiteralWords(offset, container);
				this.brlw.runningLength = 0;
				this.brlw.numberOfLiteralWords -= offset;
				this.literalWordStartPosition += offset;
				return max;
			}
			writeLiteralWords(getNumberOfLiteralWords(), container);
			index += getNumberOfLiteralWords();
			if (!next())
			{
				break;
			}
		}
		return index;
	}

	/**
	 * Write out the remain words, transforming them to zeroes.
	 *
	 * @param container
	 *            target for writes
	 */
	public void dischargeAsEmpty(final BitmapStorage container)
	{
		while (size() > 0)
		{
			container.addStreamOfEmptyWords(false, size());
			discardFirstWords(size());
		}
	}

	/**
	 * Write out up to max words (negated), returns how many were written
	 *
	 * @param container
	 *            target for writes
	 * @param max
	 *            maximal number of writes
	 * @return how many written
	 */
	public long dischargeNegated(final BitmapStorage container, final long max)
	{
		long index = 0;
		while ((index < max) && (size() > 0))
		{
			// first run
			long pl = getRunningLength();
			if (index + pl > max)
			{
				pl = max - index;
			}
			container.addStreamOfEmptyWords(!getRunningBit(), pl);
			index += pl;
			int pd = getNumberOfLiteralWords();
			if (pd + index > max)
			{
				pd = (int)(max - index);
			}
			writeNegatedLiteralWords(pd, container);
			discardFirstWords(pl + pd);
			index += pd;
		}
		return index;
	}

	/**
	 * Get the nth literal word for the current running length word
	 *
	 * @param index
	 *            zero based index
	 * @return the literal word
	 */
	@Override
	public long getLiteralWordAt(final int index)
	{
		return this.buffer.getWord(this.literalWordStartPosition + index);
	}

	/**
	 * Gets the number of literal words for the current running length word.
	 *
	 * @return the number of literal words
	 */
	@Override
	public int getNumberOfLiteralWords()
	{
		return this.brlw.numberOfLiteralWords;
	}

	/**
	 * Gets the running bit.
	 *
	 * @return the running bit
	 */
	@Override
	public boolean getRunningBit()
	{
		return this.brlw.runningBit;
	}

	/**
	 * Gets the running length.
	 *
	 * @return the running length
	 */
	@Override
	public long getRunningLength()
	{
		return this.brlw.runningLength;
	}

	/**
	 * Move to the next RunningLengthWord
	 *
	 * @return whether the move was possible
	 */
	@Override
	public boolean next()
	{
		if (!this.iterator.hasNext())
		{
			this.brlw.numberOfLiteralWords = 0;
			this.brlw.runningLength = 0;
			return false;
		}
		this.brlw.reset(this.iterator.next());
		this.literalWordStartPosition = this.iterator.literalWords(); // +
		// this.brlw.literalWordOffset
		// ==0
		return true;
	}

	/**
	 * Size in uncompressed words of the current running length word.
	 *
	 * @return the long
	 */
	@Override
	public long size()
	{
		return this.brlw.size();
	}

	/**
	 * write the first N literal words to the target bitmap. Does not discard
	 * the words or perform iteration.
	 *
	 * @param numWords
	 *            number of words to be written
	 * @param container
	 *            where we write
	 */
	public void writeLiteralWords(final int numWords, final BitmapStorage container)
	{
		container.addStreamOfLiteralWords(this.buffer, this.literalWordStartPosition, numWords);
	}

	/**
	 * write the first N literal words (negated) to the target bitmap. Does not
	 * discard the words or perform iteration.
	 *
	 * @param numWords
	 *            number of words to be written
	 * @param container
	 *            where we write
	 */
	public void writeNegatedLiteralWords(final int numWords, final BitmapStorage container)
	{
		container.addStreamOfNegatedLiteralWords(this.buffer, this.literalWordStartPosition, numWords);
	}

}
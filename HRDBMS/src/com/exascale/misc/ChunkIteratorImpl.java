package com.exascale.misc;

import static com.exascale.misc.CompressedBitSet.WORD_IN_BITS;

/**
 * The ChunkIteratorImpl is the 64 bit implementation of the ChunkIterator
 * interface, which efficiently returns the chunks of ones and zeros represented
 * by an EWAHIterator.
 *
 * @author Gregory Ssi-Yan-Kai
 */
final class ChunkIteratorImpl implements ChunkIterator
{

	private final EWAHIterator ewahIter;
	private final int sizeInBits;
	private final Buffer buffer;
	private int position;
	private boolean runningBit;
	private int runningLength;
	private long word;
	private long wordMask;
	private int wordPosition;
	private int wordLength;
	private boolean hasNext;
	private Boolean nextBit;
	private int nextLength;

	ChunkIteratorImpl(final EWAHIterator ewahIter, final int sizeInBits)
	{
		this.ewahIter = ewahIter;
		this.sizeInBits = sizeInBits;
		this.buffer = ewahIter.buffer();
		this.hasNext = moveToNextRLW();
	}

	@Override
	public boolean hasNext()
	{
		return this.hasNext;
	}

	@Override
	public void move()
	{
		move(this.nextLength);
	}

	@Override
	public void move(final int bits)
	{
		this.nextLength -= bits;
		if (this.nextLength <= 0)
		{
			do
			{
				this.nextBit = null;
				updateNext();
				this.hasNext = moveToNextRLW();
			} while (this.nextLength <= 0 && this.hasNext);
		}
	}

	@Override
	public boolean nextBit()
	{
		return this.nextBit;
	}

	@Override
	public int nextLength()
	{
		return this.nextLength;
	}

	private boolean currentWordBit()
	{
		return (this.word & this.wordMask) != 0;
	}

	private boolean hasNextRLW()
	{
		return this.ewahIter.hasNext();
	}

	private boolean literalHasNext()
	{
		while (this.word == 0 && this.wordMask == 0 && this.wordPosition < this.wordLength)
		{
			this.word = this.buffer.getWord(this.wordPosition++);
			this.wordMask = 1l;
		}
		return (this.word != 0 || this.wordMask != 0 || !hasNextRLW()) && this.position < this.sizeInBits;
	}

	private void movePosition(final int offset)
	{
		this.position += offset;
	}

	private boolean moveToNextRLW()
	{
		while (!runningHasNext() && !literalHasNext())
		{
			if (!hasNextRLW())
			{
				return this.nextBit != null;
			}
			setRLW(nextRLW());
			updateNext();
		}
		return true;
	}

	private RunningLengthWord nextRLW()
	{
		return this.ewahIter.next();
	}

	private boolean runningHasNext()
	{
		return this.position < this.runningLength;
	}

	private int runningOffset()
	{
		return this.runningLength - this.position;
	}

	private void setRLW(final RunningLengthWord rlw)
	{
		this.runningLength = Math.min(this.sizeInBits, this.position + WORD_IN_BITS * (int)rlw.getRunningLength());
		this.runningBit = rlw.getRunningBit();
		this.wordPosition = this.ewahIter.literalWords();
		this.wordLength = this.wordPosition + rlw.getNumberOfLiteralWords();
	}

	private void shiftWordMask()
	{
		this.word &= ~this.wordMask;
		this.wordMask = this.wordMask << 1;
	}

	private void updateNext()
	{
		if (runningHasNext())
		{
			if (this.nextBit == null || this.nextBit == this.runningBit)
			{
				this.nextBit = this.runningBit;
				final int offset = runningOffset();
				this.nextLength += offset;
				movePosition(offset);
				updateNext();
			}
		}
		else if (literalHasNext())
		{
			final boolean b = currentWordBit();
			if (this.nextBit == null || this.nextBit == b)
			{
				this.nextBit = b;
				this.nextLength++;
				movePosition(1);
				shiftWordMask();
				updateNext();
			}
		}
		else
		{
			moveToNextRLW();
		}
	}

}
package com.exascale.misc;

public final class EWAHIterator implements Cloneable
{

	/**
	 * The pointer represent the location of the current running length word in
	 * the array of words (embedded in the rlw attribute).
	 */
	private int pointer;

	/**
	 * The current running length word.
	 */
	final RunningLengthWord rlw;

	/**
	 * The size in words.
	 */
	private final int size;

	/**
	 * Instantiates a new EWAH iterator.
	 *
	 * @param buffer
	 *            the buffer
	 */
	public EWAHIterator(final Buffer buffer)
	{
		this.rlw = new RunningLengthWord(buffer, 0);
		this.size = buffer.sizeInWords();
		this.pointer = 0;
	}

	private EWAHIterator(final int pointer, final RunningLengthWord rlw, final int size)
	{
		this.pointer = pointer;
		this.rlw = rlw;
		this.size = size;
	}

	/**
	 * Allow expert developers to instantiate an EWAHIterator.
	 *
	 * @param bitmap
	 *            we want to iterate over
	 * @return an iterator
	 */
	public static EWAHIterator getEWAHIterator(final CompressedBitSet bitmap)
	{
		return bitmap.getEWAHIterator();
	}

	/**
	 * Access to the buffer
	 *
	 * @return the buffer
	 */
	public Buffer buffer()
	{
		return this.rlw.buffer;
	}

	@Override
	public EWAHIterator clone() throws CloneNotSupportedException
	{
		return new EWAHIterator(pointer, rlw.clone(), size);
	}

	/**
	 * Checks for next.
	 *
	 * @return true, if successful
	 */
	public boolean hasNext()
	{
		return this.pointer < this.size;
	}

	/**
	 * Position of the literal words represented by this running length word.
	 *
	 * @return the int
	 */
	public int literalWords()
	{
		return this.pointer - this.rlw.getNumberOfLiteralWords();
	}

	/**
	 * Next running length word.
	 *
	 * @return the running length word
	 */
	public RunningLengthWord next()
	{
		this.rlw.position = this.pointer;
		this.pointer += this.rlw.getNumberOfLiteralWords() + 1;
		return this.rlw;
	}

}
package com.exascale.misc;

public interface IteratingRLW
{
	/**
	 * @return a copy of the iterator
	 * @throws CloneNotSupportedException
	 *             this should not be thrown in theory
	 */
	IteratingRLW clone() throws CloneNotSupportedException;

	/**
	 * @param x
	 *            the number of words to discard
	 */
	void discardFirstWords(long x);

	/**
	 * Discard x literal words (assumes that there is no running word)
	 * 
	 * @param x
	 *            the number of words to discard
	 */
	void discardLiteralWords(long x);

	/**
	 * Discard all running words
	 */
	void discardRunningWords();

	/**
	 * @param index
	 *            where the literal word is
	 * @return the literal word at the given index.
	 */
	long getLiteralWordAt(int index);

	/**
	 * @return the number of literal (non-fill) words
	 */
	int getNumberOfLiteralWords();

	/**
	 * @return the bit used for the fill bits
	 */
	boolean getRunningBit();

	/**
	 * @return length of the run of fill words
	 */
	long getRunningLength();

	/**
	 * @return whether there is more
	 */
	boolean next();

	/**
	 * @return sum of getRunningLength() and getNumberOfLiteralWords()
	 */
	long size();
}
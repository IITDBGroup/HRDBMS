package com.exascale.misc;

interface WordArray
{
	/**
	 * Get the total number of words contained in this data structure.
	 * 
	 * @return the number
	 */
	int getNumberOfWords();

	/**
	 * Get the word at the given index
	 * 
	 * @param index
	 *            the index
	 * @return the word
	 */
	long getWord(int index);

}
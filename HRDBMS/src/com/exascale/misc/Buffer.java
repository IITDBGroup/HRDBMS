package com.exascale.misc;

interface Buffer
{

	/**
	 * Replaces the last word position in the buffer with its bitwise-and with
	 * the given mask.
	 * 
	 * @param mask
	 */
	void andLastWord(long mask);

	/**
	 * Replaces the word at the given position in the buffer with its
	 * bitwise-and with the given mask.
	 * 
	 * @param position
	 * @param mask
	 */
	void andWord(int position, long mask);

	/**
	 * Resets the buffer The buffer is not fully cleared and any new set
	 * operations should overwrite stale data
	 */
	void clear();

	/**
	 * Creates and returns a copy of the buffer
	 */
	Buffer clone();

	/**
	 * Removes a given number of words at the given position in the buffer. The
	 * freed words at the end of the buffer are properly cleaned.
	 * 
	 * @param position
	 *            the position of the buffer where to add words
	 * @param length
	 *            the number of words to add
	 */
	void collapse(int position, int length);

	/**
	 * Increases the size of the buffer if necessary
	 */
	void ensureCapacity(int capacity);

	/**
	 * Expands the buffer by adding the given number of words at the given
	 * position. The added words may contain stale data.
	 * 
	 * @param position
	 *            the position of the buffer where to add words
	 * @param length
	 *            the number of words to add
	 */
	void expand(int position, int length);

	/**
	 * Returns the last word of the buffer
	 * 
	 * @return the last word
	 */
	long getLastWord();

	/**
	 * Returns the word at a given position
	 * 
	 * @param position
	 * @return the word
	 */
	long getWord(int position);

	/**
	 * Negates the word at the given position in the buffer
	 * 
	 * @param position
	 */
	void negateWord(int position);

	/**
	 * Same as push_back, but the words are negated.
	 *
	 * @param buffer
	 *            the buffer
	 * @param start
	 *            the position of the first word to add
	 * @param number
	 *            the number of words to add
	 */
	void negative_push_back(Buffer buffer, int start, int number);

	/**
	 * Replaces the last word position in the buffer with its bitwise-or with
	 * the given mask.
	 * 
	 * @param mask
	 */
	void orLastWord(long mask);

	/**
	 * Replaces the word at the given position in the buffer with its bitwise-or
	 * with the given mask.
	 * 
	 * @param position
	 * @param mask
	 */
	void orWord(int position, long mask);

	/**
	 * Appends the specified buffer words to the end of the buffer.
	 * 
	 * @param buffer
	 *            the buffer
	 * @param start
	 *            the position of the first word to add
	 * @param number
	 *            the number of words to add
	 */
	void push_back(Buffer buffer, int start, int number);

	/**
	 * Appends the specified word to the end of the buffer
	 * 
	 * @param word
	 */
	void push_back(long word);

	/**
	 * Removes the last word from the buffer
	 */
	void removeLastWord();

	/**
	 * Replaces the last word in the buffer with the specified word.
	 * 
	 * @param word
	 */
	void setLastWord(long word);

	/**
	 * Replaces the word at the given position in the buffer with the specified
	 * word.
	 * 
	 * @param position
	 * @param word
	 */
	void setWord(int position, long word);

	/**
	 * Returns the actual size in words
	 */
	int sizeInWords();

	/**
	 * Swap the content of the buffer with another.
	 *
	 * @param other
	 *            buffer to swap with
	 */
	void swap(Buffer other);

	/**
	 * Reduces the internal buffer to its minimal allowable size. This can free
	 * memory.
	 */
	void trim();

}
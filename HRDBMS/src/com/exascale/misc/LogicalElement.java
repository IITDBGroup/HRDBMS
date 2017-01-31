package com.exascale.misc;

public interface LogicalElement<T>
{
	/**
	 * Compute the bitwise logical and
	 *
	 * @param le
	 *            element
	 * @return the result of the operation
	 */
	T and(T le);

	/**
	 * Compute the bitwise logical and not
	 *
	 * @param le
	 *            element
	 * @return the result of the operation
	 */
	T andNot(T le);

	/**
	 * Compute the composition
	 *
	 * @param le
	 *            another element
	 * @return the result of the operation
	 */
	T compose(T le);

	/**
	 * Compute the bitwise logical not (in place)
	 */
	void not();

	/**
	 * Compute the bitwise logical or
	 * 
	 * @param le
	 *            another element
	 * @return the result of the operation
	 */
	T or(T le);

	/**
	 * How many logical bits does this element represent?
	 *
	 * @return the number of bits represented by this element
	 */
	int sizeInBits();

	/**
	 * Should report the storage requirement
	 *
	 * @return How many bytes
	 * @since 0.6.2
	 */
	int sizeInBytes();

	/**
	 * Compute the bitwise logical Xor
	 *
	 * @param le
	 *            element
	 * @return the result of the operation
	 */
	T xor(T le);
}
package com.exascale.misc;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.RandomAccess;

/**
 * Resizable-array implementation of the <tt>List</tt> interface. Implements all
 * optional list operations, and permits all elements, including <tt>null</tt>.
 * In addition to implementing the <tt>List</tt> interface, this class provides
 * methods to manipulate the size of the array that is used internally to store
 * the list. (This class is roughly equivalent to <tt>Vector</tt>, except that
 * it is unsynchronized.)
 *
 * <p>
 * The <tt>size</tt>, <tt>isEmpty</tt>, <tt>get</tt>, <tt>set</tt>,
 * <tt>iterator</tt>, and <tt>listIterator</tt> operations run in constant time.
 * The <tt>add</tt> operation runs in <i>amortized constant time</i>, that is,
 * adding n elements requires O(n) time. All of the other operations run in
 * linear time (roughly speaking). The constant factor is low compared to that
 * for the <tt>LinkedList</tt> implementation.
 *
 * <p>
 * Each <tt>ArrayListLong</tt> instance has a <i>capacity</i>. The capacity is
 * the size of the array used to store the elements in the list. It is always at
 * least as large as the list size. As elements are added to an ArrayListLong,
 * its capacity grows automatically. The details of the growth policy are not
 * specified beyond the fact that adding an element has constant amortized time
 * cost.
 *
 * <p>
 * An application can increase the capacity of an <tt>ArrayListLong</tt>
 * instance before adding a large number of elements using the
 * <tt>ensureCapacity</tt> operation. This may reduce the amount of incremental
 * reallocation.
 *
 * <p>
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an <tt>ArrayListLong</tt> instance concurrently, and
 * at least one of the threads modifies the list structurally, it <i>must</i> be
 * synchronized externally. (A structural modification is any operation that
 * adds or deletes one or more elements, or explicitly resizes the backing
 * array; merely setting the value of an element is not a structural
 * modification.) This is typically accomplished by synchronizing on some object
 * that naturally encapsulates the list.
 *
 * If no such object exists, the list should be "wrapped" using the
 * {@link Collections#synchronizedList Collections.synchronizedList} method.
 * This is best done at creation time, to prevent accidental unsynchronized
 * access to the list:
 *
 * <pre>
 *   List list = Collections.synchronizedList(new ArrayListLong(...));
 * </pre>
 *
 * <p>
 * <a name="fail-fast"/> The iterators returned by this class's
 * {@link #iterator() iterator} and {@link #listIterator(int) listIterator}
 * methods are <em>fail-fast</em>: if the list is structurally modified at any
 * time after the iterator is created, in any way except through the iterator's
 * own {@link ListIterator#remove() remove} or {@link ListIterator#add(Object)
 * add} methods, the iterator will throw a
 * {@link ConcurrentModificationException}. Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than risking
 * arbitrary, non-deterministic behavior at an undetermined time in the future.
 *
 * <p>
 * Note that the fail-fast behavior of an iterator cannot be guaranteed as it
 * is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification. Fail-fast iterators throw
 * {@code ConcurrentModificationException} on a best-effort basis. Therefore, it
 * would be wrong to write a program that depended on this exception for its
 * correctness: <i>the fail-fast behavior of iterators should be used only to
 * detect bugs.</i>
 *
 * <p>
 * This class is a member of the <a href="{@docRoot}
 * /../technotes/guides/collections/index.html"> Java Collections Framework</a>.
 *
 * @author Josh Bloch
 * @author Neal Gafter
 * @see Collection
 * @see List
 * @see LinkedList
 * @see Vector
 * @since 1.2
 */

public final class ArrayListLong implements RandomAccess, Cloneable, java.io.Serializable, Iterable
{
	private static final long serialVersionUID = 8683452581122892190L;

	/**
	 * Default initial capacity.
	 */
	private static final int DEFAULT_CAPACITY = 10;

	/**
	 * Shared empty array instance used for empty instances.
	 */
	private static final long[] EMPTY_ELEMENTDATA = {};

	/**
	 * The maximum size of array to allocate. Some VMs reserve some header words
	 * in an array. Attempts to allocate larger arrays may result in
	 * OutOfMemoryError: Requested array size exceeds VM limit
	 */
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	/**
	 * The array buffer into which the elements of the ArrayListLong are stored.
	 * The capacity of the ArrayListLong is the length of this array buffer. Any
	 * empty ArrayListLong with elementData == EMPTY_ELEMENTDATA will be
	 * expanded to DEFAULT_CAPACITY when the first element is added.
	 */
	private long[] elementData;

	/**
	 * The size of the ArrayListLong (the number of elements it contains).
	 *
	 * @serial
	 */
	private int size;

	private int modCount = 0;

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public ArrayListLong()
	{
		super();
		this.elementData = EMPTY_ELEMENTDATA;
	}

	/**
	 * Constructs an empty list with the specified initial capacity.
	 *
	 * @param initialCapacity
	 *            the initial capacity of the list
	 * @throws IllegalArgumentException
	 *             if the specified initial capacity is negative
	 */
	public ArrayListLong(int initialCapacity)
	{
		super();
		if (initialCapacity < 0)
		{
			throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
		}
		this.elementData = new long[initialCapacity];
	}

	/**
	 * Constructs a list containing the elements of the specified collection, in
	 * the order they are returned by the collection's iterator.
	 *
	 * @param c
	 *            the collection whose elements are to be placed into this list
	 * @throws NullPointerException
	 *             if the specified collection is null
	 */
	public ArrayListLong(long[] c)
	{
		elementData = c;
		size = elementData.length;
	}

	private static int hugeCapacity(int minCapacity)
	{
		if (minCapacity < 0)
		{
			throw new OutOfMemoryError();
		}
		return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
	}

	/**
	 * Inserts the specified element at the specified position in this list.
	 * Shifts the element currently at that position (if any) and any subsequent
	 * elements to the right (adds one to their indices).
	 *
	 * @param index
	 *            index at which the specified element is to be inserted
	 * @param element
	 *            element to be inserted
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public void add(int index, long element)
	{
		rangeCheckForAdd(index);

		ensureCapacityInternal(size + 1); // Increments modCount!!
		System.arraycopy(elementData, index, elementData, index + 1, size - index);
		elementData[index] = element;
		size++;
	}

	/**
	 * Appends the specified element to the end of this list.
	 *
	 * @param e
	 *            element to be appended to this list
	 * @return <tt>true</tt> (as specified by {@link Collection#add})
	 */
	public boolean add(long e)
	{
		ensureCapacityInternal(size + 1); // Increments modCount!!
		elementData[size++] = e;
		return true;
	}

	/**
	 * Appends all of the elements in the specified collection to the end of
	 * this list, in the order that they are returned by the specified
	 * collection's Iterator. The behavior of this operation is undefined if the
	 * specified collection is modified while the operation is in progress.
	 * (This implies that the behavior of this call is undefined if the
	 * specified collection is this list, and this list is nonempty.)
	 *
	 * @param c
	 *            collection containing elements to be added to this list
	 * @return <tt>true</tt> if this list changed as a result of the call
	 * @throws NullPointerException
	 *             if the specified collection is null
	 */
	public boolean addAll(ArrayListLong c)
	{
		final long[] a = c.elementData;
		final int numNew = c.size;
		ensureCapacityInternal(size + numNew); // Increments modCount
		System.arraycopy(a, 0, elementData, size, numNew);
		size += numNew;
		return numNew != 0;
	}

	/**
	 * Removes all of the elements from this list. The list will be empty after
	 * this call returns.
	 */
	public void clear()
	{
		modCount++;
		size = 0;
	}

	/**
	 * Returns a shallow copy of this <tt>ArrayListLong</tt> instance. (The
	 * elements themselves are not copied.)
	 *
	 * @return a clone of this <tt>ArrayListLong</tt> instance
	 */
	@Override
	public Object clone()
	{
		try
		{
			@SuppressWarnings("unchecked")
			final ArrayListLong v = (ArrayListLong)super.clone();
			v.elementData = Arrays.copyOf(elementData, size);
			v.modCount = 0;
			return v;
		}
		catch (final CloneNotSupportedException e)
		{
			// this shouldn't happen, since we are Cloneable
			throw new InternalError();
		}
	}

	/**
	 * Returns <tt>true</tt> if this list contains the specified element. More
	 * formally, returns <tt>true</tt> if and only if this list contains at
	 * least one element <tt>e</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
	 *
	 * @param o
	 *            element whose presence in this list is to be tested
	 * @return <tt>true</tt> if this list contains the specified element
	 */
	public boolean contains(long o)
	{
		return indexOf(o) >= 0;
	}

	/**
	 * Increases the capacity of this <tt>ArrayListLong</tt> instance, if
	 * necessary, to ensure that it can hold at least the number of elements
	 * specified by the minimum capacity argument.
	 *
	 * @param minCapacity
	 *            the desired minimum capacity
	 */
	public void ensureCapacity(int minCapacity)
	{
		final int minExpand = (elementData != EMPTY_ELEMENTDATA)
		// any size if real element table
		? 0
		// larger than default for empty table. It's already supposed to be
		// at default size.
		: DEFAULT_CAPACITY;

		if (minCapacity > minExpand)
		{
			ensureExplicitCapacity(minCapacity);
		}
	}

	/**
	 * Returns the element at the specified position in this list.
	 *
	 * @param index
	 *            index of the element to return
	 * @return the element at the specified position in this list
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public long get(int index)
	{
		rangeCheck(index);

		return elementData(index);
	}

	/**
	 * Returns the index of the first occurrence of the specified element in
	 * this list, or -1 if this list does not contain the element. More
	 * formally, returns the lowest index <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 */
	public int indexOf(long o)
	{
		for (int i = 0; i < size; i++)
		{
			if (o == elementData[i])
			{
				return i;
			}
		}
		return -1;
	}

	/**
	 * Returns <tt>true</tt> if this list contains no elements.
	 *
	 * @return <tt>true</tt> if this list contains no elements
	 */
	public boolean isEmpty()
	{
		return size == 0;
	}

	/**
	 * Returns an iterator over the elements in this list in proper sequence.
	 *
	 * <p>
	 * The returned iterator is <a href="#fail-fast"><i>fail-fast</i></a>.
	 *
	 * @return an iterator over the elements in this list in proper sequence
	 */
	@Override
	public Iterator iterator()
	{
		return new Itr();
	}

	/**
	 * Returns the index of the last occurrence of the specified element in this
	 * list, or -1 if this list does not contain the element. More formally,
	 * returns the highest index <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>,
	 * or -1 if there is no such index.
	 */
	public int lastIndexOf(long o)
	{
		for (int i = size - 1; i >= 0; i--)
		{
			if (o == elementData[i])
			{
				return i;
			}
		}
		return -1;
	}

	// Positional Access Operations

	/**
	 * Removes the element at the specified position in this list. Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 *
	 * @param index
	 *            the index of the element to be removed
	 * @return the element that was removed from the list
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public long remove(int index)
	{
		rangeCheck(index);

		modCount++;
		final long oldValue = elementData(index);

		final int numMoved = size - index - 1;
		if (numMoved > 0)
		{
			System.arraycopy(elementData, index + 1, elementData, index, numMoved);
		}
		size--;

		return oldValue;
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if
	 * it is present. If the list does not contain the element, it is unchanged.
	 * More formally, removes the element with the lowest index <tt>i</tt> such
	 * that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>
	 * (if such an element exists). Returns <tt>true</tt> if this list contained
	 * the specified element (or equivalently, if this list changed as a result
	 * of the call).
	 *
	 * @param o
	 *            element to be removed from this list, if present
	 * @return <tt>true</tt> if this list contained the specified element
	 */
	public boolean remove(long o)
	{
		for (int index = 0; index < size; index++)
		{
			if (o == elementData[index])
			{
				fastRemove(index);
				return true;
			}
		}
		return false;
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 *
	 * @param index
	 *            index of the element to replace
	 * @param element
	 *            element to be stored at the specified position
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public long set(int index, long element)
	{
		rangeCheck(index);

		final long oldValue = elementData(index);
		elementData[index] = element;
		return oldValue;
	}

	/**
	 * Returns the number of elements in this list.
	 *
	 * @return the number of elements in this list
	 */
	public int size()
	{
		return size;
	}

	/**
	 * Returns an array containing all of the elements in this list in proper
	 * sequence (from first to last element).
	 *
	 * <p>
	 * The returned array will be "safe" in that no references to it are
	 * maintained by this list. (In other words, this method must allocate a new
	 * array). The caller is thus free to modify the returned array.
	 *
	 * <p>
	 * This method acts as bridge between array-based and collection-based APIs.
	 *
	 * @return an array containing all of the elements in this list in proper
	 *         sequence
	 */
	public long[] toArray()
	{
		return Arrays.copyOf(elementData, size);
	}

	/**
	 * Trims the capacity of this <tt>ArrayListLong</tt> instance to be the
	 * list's current size. An application can use this operation to minimize
	 * the storage of an <tt>ArrayListLong</tt> instance.
	 */
	public void trimToSize()
	{
		modCount++;
		if (size < elementData.length)
		{
			elementData = Arrays.copyOf(elementData, size);
		}
	}

	private void ensureCapacityInternal(int minCapacity)
	{
		if (elementData == EMPTY_ELEMENTDATA)
		{
			minCapacity = Math.max(DEFAULT_CAPACITY, minCapacity);
		}

		ensureExplicitCapacity(minCapacity);
	}

	private void ensureExplicitCapacity(int minCapacity)
	{
		modCount++;

		// overflow-conscious code
		if (minCapacity - elementData.length > 0)
		{
			grow(minCapacity);
		}
	}

	/*
	 * Private remove method that skips bounds checking and does not return the
	 * value removed.
	 */
	private void fastRemove(int index)
	{
		modCount++;
		final int numMoved = size - index - 1;
		if (numMoved > 0)
		{
			System.arraycopy(elementData, index + 1, elementData, index, numMoved);
		}
		size--;
	}

	/**
	 * Increases the capacity to ensure that it can hold at least the number of
	 * elements specified by the minimum capacity argument.
	 *
	 * @param minCapacity
	 *            the desired minimum capacity
	 */
	private void grow(int minCapacity)
	{
		// overflow-conscious code
		final int oldCapacity = elementData.length;
		int newCapacity = oldCapacity + (oldCapacity >> 1);
		if (newCapacity - minCapacity < 0)
		{
			newCapacity = minCapacity;
		}
		if (newCapacity - MAX_ARRAY_SIZE > 0)
		{
			newCapacity = hugeCapacity(minCapacity);
		}
		// minCapacity is usually close to size, so this is a win:
		elementData = Arrays.copyOf(elementData, newCapacity);
	}

	/**
	 * Constructs an IndexOutOfBoundsException detail message. Of the many
	 * possible refactorings of the error handling code, this "outlining"
	 * performs best with both server and client VMs.
	 */
	private String outOfBoundsMsg(int index)
	{
		return "Index: " + index + ", Size: " + size;
	}

	/**
	 * Checks if the given index is in range. If not, throws an appropriate
	 * runtime exception. This method does *not* check if the index is negative:
	 * It is always used immediately prior to an array access, which throws an
	 * ArrayIndexOutOfBoundsException if index is negative.
	 */
	private void rangeCheck(int index)
	{
		if (index >= size)
		{
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
		}
	}

	/**
	 * A version of rangeCheck used by add and addAll.
	 */
	private void rangeCheckForAdd(int index)
	{
		if (index > size || index < 0)
		{
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
		}
	}

	@SuppressWarnings("unchecked")
	long elementData(int index)
	{
		return elementData[index];
	}

	/**
	 * An optimized version of AbstractList.Itr
	 */
	private final class Itr implements Iterator
	{
		int cursor; // index of next element to return
		int lastRet = -1; // index of last element returned; -1 if no such
		int expectedModCount = modCount;

		@Override
		public boolean hasNext()
		{
			return cursor != size;
		}

		@Override
		@SuppressWarnings("unchecked")
		public Long next()
		{
			checkForComodification();
			final int i = cursor;
			if (i >= size)
			{
				throw new NoSuchElementException();
			}
			final long[] elementData = ArrayListLong.this.elementData;
			if (i >= elementData.length)
			{
				throw new ConcurrentModificationException();
			}
			cursor = i + 1;
			return new Long(elementData[lastRet = i]);
		}

		@Override
		public void remove()
		{
			if (lastRet < 0)
			{
				throw new IllegalStateException();
			}
			checkForComodification();

			try
			{
				ArrayListLong.this.remove(lastRet);
				cursor = lastRet;
				lastRet = -1;
				expectedModCount = modCount;
			}
			catch (final IndexOutOfBoundsException ex)
			{
				throw new ConcurrentModificationException();
			}
		}

		final void checkForComodification()
		{
			if (modCount != expectedModCount)
			{
				throw new ConcurrentModificationException();
			}
		}
	}
}

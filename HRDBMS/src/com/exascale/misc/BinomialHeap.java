package com.exascale.misc;

import java.util.AbstractQueue;
import java.util.Comparator;
import java.util.Iterator;

public final class BinomialHeap<E> extends AbstractQueue<E>
{

	private final Node<E> head;
	private final Comparator compare;

	public BinomialHeap(Comparator compare)
	{
		head = new Node<E>(); // Dummy node
		this.compare = compare;
	}

	@Override
	public void clear()
	{
		head.next = null;
	}

	public E extractMin()
	{
		return poll();
	}

	public void insert(E val)
	{
		offer(val);
	}

	// Can't support min-order traversal in place; would need to clone the heap
	@Override
	public Iterator<E> iterator()
	{
		throw new UnsupportedOperationException();
	}

	// Moves all the values in the given heap into this heap
	public void merge(BinomialHeap<E> other)
	{
		if (other == this)
		{
			throw new IllegalArgumentException();
		}
		merge(other.head.next);
		other.head.next = null;
	}

	@Override
	public boolean offer(E val)
	{
		merge(new Node<E>(val));
		return true;
	}

	@Override
	public E peek()
	{
		E result = null;
		for (Node<E> node = head.next; node != null; node = node.next)
		{
			if (result == null || compare.compare(node.value, result) < 0)
			{
				result = node.value;
			}
		}
		return result;
	}

	@Override
	public E poll()
	{
		if (head.next == null)
		{
			return null;
		}
		E min = null;
		Node<E> nodeBeforeMin = null;
		for (Node<E> node = head.next, prevNode = head; node != null; prevNode = node, node = node.next)
		{
			if (min == null || compare.compare(node.value, min) < 0)
			{
				min = node.value;
				nodeBeforeMin = prevNode;
			}
		}
		// assert min != null && nodeBeforeMin != null;

		Node<E> minNode = nodeBeforeMin.next;
		nodeBeforeMin.next = minNode.next;
		minNode.next = null;
		merge(minNode.removeRoot());
		return min;
	}

	@Override
	public int size()
	{
		int result = 0;
		for (Node<?> node = head.next; node != null; node = node.next)
		{
			if (node.rank >= 31)
			{
				throw new ArithmeticException("Size overflow"); // The result
				// cannot be
				// returned,
				// however the
				// data
				// structure is
				// still valid
			}
			result |= 1 << node.rank;
		}
		return result;
	}

	// 'other' must not start with a dummy node
	private void merge(Node<E> other)
	{
		// assert head.rank == -1;
		Node<E> self = head.next;
		head.next = null;
		Node<E> prevTail = null;
		Node<E> tail = head;

		while (self != null || other != null)
		{
			Node<E> node;
			if (other == null || self != null && self.rank <= other.rank)
			{
				node = self;
				self = self.next;
			}
			else
			{
				node = other;
				other = other.next;
			}
			node.next = null;

			// assert tail.next == null;
			if (tail.rank < node.rank)
			{
				prevTail = tail;
				tail.next = node;
				tail = node;
			}
			else if (tail.rank == node.rank + 1)
			{
				// assert prevTail != null;
				node.next = tail;
				prevTail.next = node;
				prevTail = node;
			}
			else if (tail.rank == node.rank)
			{
				// Merge nodes
				if (compare.compare(tail.value, node.value) <= 0)
				{
					node.next = tail.down;
					tail.down = node;
					tail.rank++;
				}
				else
				{
					// assert prevTail != null;
					tail.next = node.down;
					node.down = tail;
					node.rank++;
					tail = node;
					prevTail.next = node;
				}
			}
			else
			{
				throw new AssertionError();
			}
		}
	}

	// For unit tests
	void checkStructure()
	{
		if (head.value != null || head.rank != -1)
		{
			throw new AssertionError();
		}
		if (head.next != null)
		{
			if (head.next.rank <= head.rank)
			{
				throw new AssertionError();
			}
			head.next.checkStructure(true);
		}
	}

	private static final class Node<E>
	{

		public E value;
		public int rank;

		public Node<E> down;
		public Node<E> next;

		// Dummy sentinel node at head of list
		public Node()
		{
			this(null);
			rank = -1;
		}

		// Regular node
		public Node(E val)
		{
			value = val;
			rank = 0;
			down = null;
			next = null;
		}

		public Node<E> removeRoot()
		{
			// assert next == null;
			Node<E> result = null;
			Node<E> node = down;
			while (node != null)
			{ // Reverse the order of nodes from descending rank to ascending
				// rank
				Node<E> next = node.next;
				node.next = result;
				result = node;
				node = next;
			}
			return result;
		}

		// For unit tests
		void checkStructure(boolean isMain)
		{
			if (value == null || rank < 0)
			{
				throw new AssertionError();
			}
			if (rank >= 1)
			{
				if (down == null || down.rank != rank - 1)
				{
					throw new AssertionError();
				}
				down.checkStructure(false);
				if (!isMain)
				{
					if (next == null || next.rank != rank - 1)
					{
						throw new AssertionError();
					}
					next.checkStructure(false);
				}
			}
			if (isMain && next != null)
			{
				if (next.rank <= rank)
				{
					throw new AssertionError();
				}
				next.checkStructure(true);
			}
		}

	}

}
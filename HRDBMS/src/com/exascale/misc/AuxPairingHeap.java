package com.exascale.misc;

import java.util.Comparator;

public final class AuxPairingHeap<E>
{
	private final Comparator compare;
	private Node node = null;
	private Node aux = null;
	private Node minPtr = null;
	private Node minAuxPtr = null;
	private int size = 0;

	public AuxPairingHeap(Comparator compare)
	{
		this.compare = compare;
	}

	public E extractMin()
	{
		E retval = minPtr.data;

		if (minPtr != node)
		{
			if (minPtr == aux)
			{
				aux = minPtr.right;
				if (aux != null)
				{
					aux.left = null;
				}
			}
			else
			{
				minPtr.left.right = minPtr.right;

				if (minPtr.right != null)
				{
					minPtr.right.left = minPtr.left;
				}
			}

			node = merge(node, mergePairsMP(aux));
			minPtr = node;
			aux = null;
			minAuxPtr = null;
		}
		else
		{
			node = mergePairs(node.child);
			
			if (minAuxPtr == null)
			{
				minPtr = node;
			}
			else
			{
				if (node == null)
				{
					node = mergePairsMP(aux);
					minPtr = node;
					aux = null;
					minAuxPtr = null;
				}
				else if (compare.compare(node.data, minAuxPtr.data) < 0)
				{
					minPtr = node;
				}
				else
				{
					minPtr = minAuxPtr;
				}
			}
		}

		size--;
		return retval;
	}

	public E findMin()
	{
		return minPtr.data;
	}

	public void insert(E val)
	{
		Node valNode = new Node(val);
		size++;

		if (node == null)
		{
			node = valNode;
			minPtr = valNode;
			return;
		}
		else if (aux == null)
		{
			aux = valNode;
			minAuxPtr = valNode;
		}
		else
		{
			valNode.right = aux;
			aux.left = valNode;
			aux = valNode;
		}

		if (compare.compare(valNode.data, minPtr.data) < 0)
		{
			minPtr = valNode;
			minAuxPtr = valNode;
		}
		else if (minPtr == node && minAuxPtr != valNode && compare.compare(valNode.data, minAuxPtr.data) < 0)
		{
			minAuxPtr = valNode;
		}
	}

	public int size()
	{
		return size;
	}

	private Node merge(Node heap1, Node heap2)
	{
		if (heap2 == null)
		{
			return heap1;
		}
		else if (compare.compare(heap1.data, heap2.data) < 0)
		{
			heap2.right = heap1.child;

			if (heap1.child != null)
			{
				heap1.child.left = heap2;
			}

			heap1.child = heap2;
			return heap1;
		}
		else
		{
			heap1.right = heap2.child;

			if (heap2.child != null)
			{
				heap2.child.left = heap1;
			}

			heap2.child = heap1;
			return heap2;
		}
	}

	private Node mergePairs(Node list)
	{
		if (list == null)
		{
			return null;
		}

		if (list.right == null)
		{
			return list;
		}

		Node right = list.right;
		Node rightRight = right.right;
		list.right = null;
		right.right = null;
		right.left = null;

		if (rightRight != null)
		{
			rightRight.left = null;
		}

		return merge(merge(list, right), mergePairs(rightRight));
	}

	private Node mergePairsMP(Node list)
	{
		if (list == null)
		{
			return null;
		}

		if (list.right == null)
		{
			return list;
		}

		Node last = null;
		Node head = null;

		while (true)
		{
			Node current = list;
			Node right = current.right;

			if (right == null && head != null)
			{
				right = head;
				head = null;
			}

			list = right.right;
			current.right = null;
			right.right = null;
			right.left = null;

			if (list != null)
			{
				list.left = null;
			}

			Node newNode = merge(current, right);
			if (list == null)
			{
				if (head == null)
				{
					return newNode;
				}
				else
				{
					list = head;
					head = null;
					last.right = newNode;
					last = newNode;
				}
			}
			else
			{
				if (last == null)
				{
					last = newNode;
					head = newNode;
				}
				else
				{
					last.right = newNode;
					last = newNode;
				}
			}
		}
	}

	private class Node
	{
		private final E data;
		private Node child;
		private Node right;
		private Node left;

		public Node(E data)
		{
			this.data = data;
		}
	}
}
package com.exascale.misc;

import java.util.Comparator;

public class BinomialHeap<K> {

	// internal class BinomialHeapNode
	public class BinomialHeapNode<K> {

		private K key; // element in current node

		private int degree; // depth of the binomial tree having the current node as its root

		private BinomialHeapNode<K> parent; // pointer to the parent of the current node

		private BinomialHeapNode<K> sibling; // pointer to the next binomial tree in the list

		private BinomialHeapNode<K> child; // pointer to the first child of the current node

		public BinomialHeapNode(K k) {
			//	public BinomialHeapNode(Integer k) {
			key = k;
			degree = 0;
			parent = null;
			sibling = null;
			child = null;
		}

		public K getKey() { // returns the element in the current node
			return key;
		}

		private void setKey(K value) { // sets the element in the current node
			key = value;
		}

		public int getDegree() { // returns the degree of the current node
			return degree;
		}

		private void setDegree(int deg) { // sets the degree of the current node
			degree = deg;
		}

		public BinomialHeapNode<K> getParent() { // returns the father of the current node
			return parent;
		}

		private void setParent(BinomialHeapNode<K> par) { // sets the father of the current node
			parent = par;
		}

		public BinomialHeapNode<K> getSibling() { // returns the next binomial tree in the list
			return sibling;
		}

		private void setSibling(BinomialHeapNode<K> nextBr) { // sets the next binomial tree in the list
			sibling = nextBr;
		}

		public BinomialHeapNode<K> getChild() { // returns the first child of the current node
			return child;
		}

		private void setChild(BinomialHeapNode<K> firstCh) { // sets the first child of the current node
			child = firstCh;
		}

		public int getSize() {
			return (1 + ((child == null) ? 0 : child.getSize()) + ((sibling == null) ? 0
					: sibling.getSize()));
		}

		private BinomialHeapNode<K> reverse(BinomialHeapNode<K> sibl) {
			BinomialHeapNode<K> ret;
			if (sibling != null)
				ret = sibling.reverse(this);
			else
				ret = this;
			sibling = sibl;
			return ret;
		}

		private BinomialHeapNode<K> findMinNode() {
			BinomialHeapNode<K> x = this, y = this;
			K min = x.key;
			Object a, b;

			while (x != null) {
				if (comparator.compare(x.key, min) == -1) {
					y = x;
					min = (K)x.key;
				}
				x = x.sibling;
			}

			return y;
		}

		// Find a node with the given key
		private BinomialHeapNode<K> findANodeWithKey(K value) {
			BinomialHeapNode<K> temp = this, node = null;
			while (temp != null) {
				if (temp.key == value) {
					node = temp;
					break;
				}
				if (temp.child == null)
					temp = temp.sibling;
				else {
					node = temp.child.findANodeWithKey(value);
					if (node == null)
						temp = temp.sibling;
					else
						break;
				}
			}

			return node;
		}

	}

	private BinomialHeapNode<K> Nodes;
	private Comparator<Object> comparator;
	private BinomialHeapNode<K> minNode;

	private int size;

	public BinomialHeap(Comparator<Object> comparator) {
		this.comparator = comparator;
		Nodes = null;
		size = 0;
	}

	// 2. Find the minimum key
	public K findMinimum() {
		//return Nodes.findMinNode().key;
		return minNode.key;
	}

	// 3. Unite two binomial heaps
	// helper procedure
	private void merge(BinomialHeapNode<K> binHeap) {
		BinomialHeapNode<K> temp1 = Nodes, temp2 = binHeap;
		while ((temp1 != null) && (temp2 != null)) {
			if (temp1.degree == temp2.degree) {
				BinomialHeapNode<K> tmp = temp2;
				temp2 = temp2.sibling;
				tmp.sibling = temp1.sibling;
				temp1.sibling = tmp;
				temp1 = tmp.sibling;
			} else {
				if (temp1.degree < temp2.degree) {
					if ((temp1.sibling == null)
							|| (temp1.sibling.degree > temp2.degree)) {
						BinomialHeapNode<K> tmp = temp2;
						temp2 = temp2.sibling;
						tmp.sibling = temp1.sibling;
						temp1.sibling = tmp;
						temp1 = tmp.sibling;
					} else {
						temp1 = temp1.sibling;
					}
				} else {
					BinomialHeapNode<K> tmp = temp1;
					temp1 = temp2;
					temp2 = temp2.sibling;
					temp1.sibling = tmp;
					if (tmp == Nodes) {
						Nodes = temp1;
					} else {
					}
				}
			}
		}

		if (temp1 == null) {
			temp1 = Nodes;
			while (temp1.sibling != null) {
				temp1 = temp1.sibling;
			}
			temp1.sibling = temp2;
		} else {
		}
	}

	// another helper procedure
	private void unionNodes(BinomialHeapNode<K> binHeap) {
		merge(binHeap);

		BinomialHeapNode<K> prevTemp = null, temp = Nodes, nextTemp = Nodes.sibling;

		while (nextTemp != null) {
			if ((temp.degree != nextTemp.degree)
					|| ((nextTemp.sibling != null) && (nextTemp.sibling.degree == temp.degree))) {
				prevTemp = temp;
				temp = nextTemp;
			} else {
				if (comparator.compare(temp.key, nextTemp.key) < 1) {
					temp.sibling = nextTemp.sibling;
					nextTemp.parent = temp;
					nextTemp.sibling = temp.child;
					temp.child = nextTemp;
					temp.degree++;
				} else {
					if (prevTemp == null) {
						Nodes = nextTemp;
					} else {
						prevTemp.sibling = nextTemp;
					}
					temp.parent = nextTemp;
					temp.sibling = nextTemp.child;
					nextTemp.child = temp;
					nextTemp.degree++;
					temp = nextTemp;
				}
			}

			nextTemp = temp.sibling;
		}
	}

	// 4. Insert a node with a specific value
	public void insert(K value) {
		BinomialHeapNode<K> temp = new BinomialHeapNode<K>(value);
		if (Nodes == null) {
			Nodes = temp;
			minNode = temp;
			size = 1;
		} else {
			unionNodes(temp);
			if (comparator.compare(temp.key, minNode.key) == -1)
			{
				minNode = temp;
			}
			size++;
		}
	}

	// 5. Extract the node with the minimum key
	public K extractMin() {
		if (Nodes == null)
			return null;

		BinomialHeapNode<K> temp = Nodes, prevTemp = null;
		while (temp != minNode) {
			prevTemp = temp;
			temp = temp.sibling;
		}

		if (prevTemp == null) {
			Nodes = temp.sibling;
		} else {
			prevTemp.sibling = temp.sibling;
		}
		temp = temp.child;
		BinomialHeapNode<K> fakeNode = temp;
		while (temp != null) {
			temp.parent = null;
			temp = temp.sibling;
		}

		if ((Nodes == null) && (fakeNode == null)) {
			size = 0;
		} else {
			if ((Nodes == null) && (fakeNode != null)) {
				Nodes = fakeNode.reverse(null);
				size = Nodes.getSize();
			} else {
				if ((Nodes != null) && (fakeNode == null)) {
					size = Nodes.getSize();
				} else {
					unionNodes(fakeNode.reverse(null));
					size = Nodes.getSize();
				}
			}
		}

		K retval = minNode.key;
		minNode = Nodes.findMinNode();
		return retval;
	}
	
	public int size()
	{
		return size;
	}
}
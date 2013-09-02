import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;

public class MyVector<E> extends Vector<E> implements Serializable
{
	/**
	 * 
	 */
	private static final long serialVersionUID = -7156170534070898795L;
	private Vector<E> internal;
	private int pageSize = 500000;
	public int pages;
	private int hash;
	private int size;
	private int currentPage;
	
	private void readObject(
		     ObjectInputStream aInputStream
		   ) throws ClassNotFoundException, IOException {
		     //always perform the default de-serialization first
		     aInputStream.defaultReadObject();
		  }

		    /**
		    * This is the default implementation of writeObject.
		    * Customise if necessary.
		    */
		    private void writeObject(
		      ObjectOutputStream aOutputStream
		    ) throws IOException {
		      //perform the default serialization for all non-transient, non-static fields
		      aOutputStream.defaultWriteObject();
		    }
	
	public MyVector()
	{
		internal = new Vector();
		pages = 1;
		size = 0;
		currentPage = 0;
		hash = (int)(System.currentTimeMillis() % Integer.MAX_VALUE);
	}
	
	public MyVector(int pageSize)
	{
		this.pageSize = pageSize;
		internal = new Vector();
		pages = 1;
		size = 0;
		currentPage = 0;
		hash = (int)(System.currentTimeMillis() % Integer.MAX_VALUE);
	}
	
	public boolean	add(E e) 
	{
		if (!lastPageIsFull())
		{
			size++;
			return internal.add(e);
		}
		
		saveCurrentPage();
		newPage();
		size++;
		return internal.add(e);
	}
	
	private void saveCurrentPage()
	{
		try
		{
			ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream("A" + hash + "_" + currentPage, false)));
			out.writeObject(internal);
			out.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
	
	private void newPage()
	{
		internal.clear();
		currentPage++;
		pages++;
	}
	
	private boolean lastPageIsFull()
	{
		if (!(currentPage == (pages-1)))
		{
			setPage(pages-1);
		}
		return (internal.size() == pageSize);
		
	}
	
	public void	add(int index, E element) 
	{
		System.out.println("add(int, Object) is not supported");
	}

	public boolean	addAll(Collection<? extends E> c) 
	{
		for (E item : c)
		{
			add(item);
		}
		
		return true;
	}
	
	public boolean	addAll(int index, Collection<? extends E> c)
	{
		System.out.println ("addAll(int, Collection) is not supported");
		return false;
	}
    
	public void	addElement(E obj)
	{
		add(obj);
	}
    

	public int	capacity()
	{
		return pages * pageSize;
	}
	

	public void	clear()
	{
		internal.clear();
		pages = 1;
		currentPage = 0;
		size = 0;
	}
    
	public Object	clone()
	{
		System.out.println("clone() is not supported");
		return null;
	}
    
	public boolean	contains(Object o)
	{
		System.out.println("contains(Object) is not supported");
		return false;
	}

	public boolean	containsAll(Collection<?> c)
	{
		System.out.println("containsAll(Collection) is not supported");
		return false;
	}
    
	public void	copyInto(Object[] anArray)
	{
		System.out.println("copyInto is not supported");
	}
    
	public E	elementAt(int index)
	{
		return get(index);
	}
    
	public Enumeration<E>	elements()
	{
		System.out.println("elements() is not supported");
		return null;
	}
    
	public void	ensureCapacity(int minCapacity)
	{
		internal.ensureCapacity(minCapacity);
	}
 
	public boolean	equals(Object o)
	{
		return (this == o);
	}
	
    
	public E	firstElement()
	{
		return get(0);
	}
    
	public int	hashCode()
	{
		return hash;
	}
    
	public int	indexOf(Object o) 
	{
		System.out.println("indexOf(Object) is not supported");
		return -1;
	}
	
	public int	indexOf(Object o, int index) 
    {
		System.out.println("indexOf(Object, int) is not supported");
		return -1;
    }
	
	public void	insertElementAt(E obj, int index)
	{
		System.out.println("insertElementAt(Object, int) is not supported");
	}
	

	public boolean	isEmpty()
	{
		return (size() == 0);
	}
	
	public E	lastElement()
	{
		return get(size() - 1);
	}
    
	public int	lastIndexOf(Object o)
	{
		System.out.println("lastIndexOf(Object) is not supported");
		return -1;
	}
    
	public int	lastIndexOf(Object o, int index)
	{
		System.out.println("lastIndexOf(Object, int) is not supported");
		return -1;
	}
    
	public E	remove(int index)
	{
		System.out.println("remove(int) is not supported");
		return null;
	}
	
	public boolean	remove(Object o)
	{
		System.out.println("remove(Object) is not supported");
		return false;
	}
    
	public boolean	removeAll(Collection<?> c)
	{
		System.out.println("removeAll(Collection) is not supported");
		return false;
	}
	
	public void	removeAllElements()
	{
		clear();
	}
    
	public boolean	removeElement(Object obj)
	{
		System.out.println("removeElement(Object) is not supported");
		return false;
	}
    
	public void	removeElementAt(int index)
	{
		System.out.println("removeElementAt(int) is not supported");
	}
    
	protected  void	removeRange(int fromIndex, int toIndex)
	{
		System.out.println("removeRange(int, int) is not supported");
	}
    
	public boolean	retainAll(Collection<?> c)
	{
		System.out.println("retainAll(Collection) is not supported");
		return false;
	}
    
	public E	set(int index, E element)
	{
		System.out.println("set(int, Object) is not supported");
		return null;
	}
    
	public void	setElementAt(E obj, int index)
	{
		System.out.println("setElementAt(Object, int) is not supported");
	}
    
	public void	setSize(int newSize)
	{
		System.out.println("setSize(int) is not supported");
	}
    
	public int	size()
	{
		return size;
	}
    
	public List<E>	subList(int fromIndex, int toIndex)
	{
		System.out.println("sublist(int, int) is not supported");
		return null;
	}
    
	public Object[]	toArray()
	{
		System.out.println("toArray() is not supported");
		return null;
	}
    
	public <T> T[] toArray(T[] a) 
	{
		System.out.println("toArray(Object[]) is not supported");
		return null;
	}
    
	public String	toString()
	{
		return internal.toString() + " plus " + (pages - 1) + " other pages";
	}
   
	public void	trimToSize()
	{
		System.out.println("trimToSize() is not supported");
	}
    
    public E	get(int index) 
    {
    	int page = index / pageSize;
    	int off = index % pageSize;
    	
    	if (page != currentPage && currentPage == (pages - 1))
    	{
    		saveCurrentPage();
    	}
    	
    	if (page == currentPage)
    	{
    		return internal.get(off);
    	}
    	
    	setPage(page);
    	return internal.get(off);
    }
    
    public MyVector getPage(int index)
    {
    	
    	if (currentPage == (pages - 1))
    	{
    		saveCurrentPage();
    		setPage(0);
    	}
    	
    	try
    	{
    		ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("A" + hash + "_" + index)));
    		Vector temp = (Vector)in.readObject();
    		in.close();
    		MyVector retval = new MyVector();
    		retval.addAll(temp);
    		return retval;
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    		return null;
    	}
    }
    
    private void setPage(int index)
    {
    	if (index == currentPage)
    	{
    		return;
    	}
    	
    	try
    	{
    		ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream("A" + hash + "_" + index)));
    		internal = (Vector)in.readObject();
    		in.close();
    		currentPage = index;
    	}
    	catch(Exception e)
    	{
    		e.printStackTrace();
    	}
    }
    
    public Iterator iterator()
    {
    	return new MyVectorIterator(this);
    }
    
    public ListIterator listIterator()
    {
    	System.out.println("listIterator() is not supported");
    	return null;
    }
    
    public ListIterator listIterator(int index)
    {
    	System.out.println("listIterator(int) is not supported");
    	return null;
    }
    
    private class MyVectorIterator implements Iterator
    {
    	private int pos;
    	private MyVector vector;
    	
    	public MyVectorIterator(MyVector vector)
    	{
    		this.vector = vector;
    		pos = 0;
    	}

		@Override
		public boolean hasNext() {
			
			return (pos < vector.size);
		}

		@Override
		public Object next() {
			Object temp = vector.get(pos);
			pos++;
			return temp;
		}

		@Override
		public void remove() {
			System.out.println("MyVectorIterator.remove() is not supported");
			
		}
    	
    }
}

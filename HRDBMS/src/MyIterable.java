import java.util.Iterator;


public class MyIterable implements Iterable
{
	private Iterable iter;
	private ResultSetWritable first;
	private Iterator iterator;
	private Iterator iteriter;

	public MyIterable(Iterable iter)
	{
		this.iter = iter;
		iteriter = iter.iterator();
	}
	
	public ResultSetWritable browse()
	{
		first = (ResultSetWritable)iteriter.next();
		return first;
	}
	
	public Iterator iterator()
	{
		return getIterator();
	}
	
	private Iterator getIterator()
	{
		if (iterator != null)
		{
			return iterator;
		}
		
		iterator = new MyIterator(first, iteriter);
		return iterator;
	}
	
	private class MyIterator implements Iterator
	{
		private ResultSetWritable first;
		private Iterator iter;
		
		public MyIterator(ResultSetWritable first, Iterator iter)
		{
			this.first = first;
			this.iter = iter;
		}

		@Override
		public boolean hasNext() {
			return (first != null || iter.hasNext());
		}

		@Override
		public Object next() {
			if (first != null)
			{
				ResultSetWritable temp = first;
				first = null;
				return temp;
			}
			
			ResultSetWritable temp = (ResultSetWritable)iter.next();
			return temp;
		}

		@Override
		public void remove() {
			// TODO Auto-generated method stub
			iter.remove();
		}
	}
}

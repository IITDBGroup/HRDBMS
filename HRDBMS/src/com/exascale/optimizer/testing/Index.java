package com.exascale.optimizer.testing;

import java.io.RandomAccessFile;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class Index implements Serializable
{
	protected String fileName;
	protected ArrayList<String> keys;
	protected ArrayList<String> types;
	protected ArrayList<Boolean> orders;
	protected Filter f;
	protected ArrayList<Filter> secondary = new ArrayList<Filter>();
	protected BufferedRandomAccessFile in;
	protected boolean positioned = false;
	protected ArrayListLong ridList = new ArrayListLong();
	protected String col;
	protected String op;
	protected ArrayList<Filter> terminates = new ArrayList<Filter>();
	protected String line;
	//protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	protected HashMap<String, Integer> cols2Pos = new HashMap<String, Integer>();
	protected int count = 0;
	protected boolean indexOnly = false;
	protected ArrayList<Integer> fetches = new ArrayList<Integer>();
	protected ArrayList<String> fetchTypes = new ArrayList<String>();
	protected ArrayList<Object> row;
	protected volatile boolean delayed = false;
	protected int device;
	protected MetaData meta;
	protected ArrayList<Filter> delayedConditions;
	protected HashMap<String, String> renames;
	
	public Index(String fileName, ArrayList<String> keys, ArrayList<String> types, ArrayList<Boolean> orders)
	{
		this.fileName = fileName;
		this.keys = keys;
		this.types = types;
		this.orders = orders;
		int i = 0;
		for (String key : keys)
		{
			cols2Pos.put(key, i);
			i++;
		}
	}
	
	public Index clone()
	{
		Index retval = new Index(fileName, keys, types, orders);
		retval.f = this.f;
		retval.secondary = (ArrayList<Filter>)secondary.clone();
		retval.indexOnly = indexOnly;
		retval.fetches = fetches;
		retval.fetchTypes = fetchTypes;
		retval.delayed = delayed;
		retval.renames = renames;
		return retval;
	}
	
	public void setRenames(HashMap<String, String> renames)
	{
		this.renames = renames;
	}
	
	public void setDelayedConditions(ArrayList<Filter> filters)
	{
		this.delayedConditions = filters;
		for (Filter filter : filters)
		{
			if (renames != null)
			{
				if (filter.leftIsColumn() && renames.containsKey(filter.leftColumn()))
				{
					filter.updateLeftColumn(renames.get(filter.leftColumn()));
				}
				
				if (filter.rightIsColumn() && renames.containsKey(filter.rightColumn()))
				{
					filter.updateRightColumn(renames.get(filter.rightColumn()));
				}
			}
			if (f == null)
			{
				f = filter;
			}
			else
			{
				secondary.add(filter);
			}
		}
		
		delayed = false;
		//System.out.println("Starting index scan after delay");
	}
	
	public void open(int device, MetaData meta)
	{
		try
		{
			in = new BufferedRandomAccessFile(meta.getDevicePath(device) + fileName, "r", 512);
			this.device = device;
			this.meta = meta;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		if (delayed)
		{
			System.out.println("Index is opened delayed");
		}
	}
	
	public void close()
	{
		try
		{
			in.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	public Object next()
	{
		while (delayed)
		{
			try
			{
				Thread.sleep(1);
			}
			catch(InterruptedException e)
			{}
		}
		
		if (!positioned)
		{
			setStartingPos();
			positioned = true;
		}
		
		if (!indexOnly)
		{
			if (ridList.size() > 0)
			{
				ArrayList<Object> retval = new ArrayList<Object>(1);
				retval.add(ridList.remove(0));
				count++;
				return retval;
			}
		
			ArrayList<Object> retval = null;
			try
			{
				while (!currentKeySatisfies())
				{
					if (marksEnd())
					{
						//System.out.println("Index " + fileName + "(" + f + ") returned " + count + " RIDs");
						return new DataEndMarker();
					}
			
					line = in.readLine();
				}
			
				ridList.addAll(getRids());
				retval = new ArrayList<Object>(1);
				retval.add(ridList.remove(0));
				count++;
				line = in.readLine();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return retval;
		}
		else
		{
			if (ridList.size() > 0)
			{
				ridList.remove(0);
				count++;
				return row.clone();
			}
		
			row = new ArrayList<Object>(fetches.size());
			try
			{
				while (!currentKeySatisfies())
				{
					if (marksEnd())
					{
						//System.out.println("Index " + fileName + "(" + f + ") returned " + count + " RIDs");
						return new DataEndMarker();
					}
			
					line = in.readLine();
				}
			
				ridList.addAll(getRids());
				int i = 0;
				int j = 0;
				FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
				for (int pos : fetches)
				{
					while (i < pos)
					{
						tokens.nextToken();
						i++;
					}
					
					String val = tokens.nextToken();
					i++;
					String type = fetchTypes.get(j);
					if (type.equals("INT"))
					{
						row.add(ResourceManager.internInt(Integer.parseInt(val)));
					}
					else if (type.equals("FLOAT"))
					{
						row.add(ResourceManager.internDouble(Double.parseDouble(val)));
					}
					else if (type.equals("CHAR"))
					{
						row.add(ResourceManager.internString(val));
					}
					else if (type.equals("LONG"))
					{
						row.add(ResourceManager.internLong(Long.parseLong(val)));
					}
					else if (type.equals("DATE"))
					{
						row.add(DateParser.parse(val));
					}
					
					j++;
				}
				
				ridList.remove(0);
				count++;
				line = in.readLine();
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
			return row.clone();
		}
	}
	
	public void setCondition(Filter f)
	{
		this.f = f;
	}
	
	public Filter getCondition()
	{
		return f;
	}
	
	public boolean startsWith(String col)
	{
		if (keys.get(0).equals(col))
		{
			return true;
		}
		
		return false;
	}
	
	public boolean contains(String col)
	{
		return keys.contains(col);
	}
	
	private void setStartingPos()
	{
		long off = -1;
		try
		{
			line = in.readLine();
			off = Long.parseLong(line);
			in.seek(-1 * off);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		boolean doFirst = true;
		Object equalVal = null;
		col = null;
		op = f.op();
		Object val = null;
		String orig = null;
		if (f.leftIsColumn())
		{
			col = f.leftColumn();
			val = f.rightLiteral();
			orig = f.rightOrig();
		}
		else
		{
			col = f.rightColumn();
			val = f.leftLiteral();
			orig = f.leftOrig();
			switchOp();
		}
		
		if (keys.get(0).equals(col) && (!f.leftIsColumn() || !f.rightIsColumn()))
		{
			try
			{
				if (op.equals("L"))
				{
					if (orders.get(0))
					{
						//setFirstPosition();
						terminates.add(new Filter(col, "GE", orig));
					}
					else
					{
						//setEqualsPos(val);
						doFirst = false;
					}
				}
				else if (op.equals("LE"))
				{
					if (orders.get(0))
					{
						//setFirstPosition();
						terminates.add(new Filter(col, "G", orig));
					}
					else
					{
						//setEqualsPos(val);
						doFirst = false;
					}
				}
				else if (op.equals("G"))
				{
					if (orders.get(0))
					{
						//setEqualsPos(val);
						doFirst = false;
					}
					else
					{
						//setFirstPosition();
						terminates.add(new Filter(col, "LE", orig));
					}
				}
				else if (op.equals("GE"))
				{
					if (orders.get(0))
					{
						//setEqualsPos(val);
						doFirst = false;
					}
					else
					{
						//setFirstPosition();
						terminates.add(new Filter(col, "L", orig));
					}
				}
				else if (op.equals("E"))
				{
					//setEqualsPos(val);
					doFirst = false;
					terminates.add(new Filter(col, "NE", orig));
				}
				else if (op.equals("NE"))
				{
					//setFirstPosition();
				}
				else if (op.equals("LI"))
				{
					String prefix = getPrefix(val);
					if (prefix.length() > 0)
					{
						if (orders.get(0))
						{
							//setEqualsPos(prefix);
							doFirst = false;
							equalVal = prefix;
							terminates.add(new Filter(col, "GE", "'" + nextGT(prefix) + "'"));
						}
						else
						{
							//setEqualsPos(nextGT(prefix));
							doFirst = false;
							equalVal = nextGT(prefix);
							terminates.add(new Filter(col, "L", "'" + prefix + "'"));
						}
					}
					else
					{
						//setFirstPosition();
					}
				}
				else if (op.equals("NL"))
				{
					//setFirstPosition();
				}
				else
				{
					System.out.println("Unknown operator in Index: " + op);
					System.exit(1);
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		else
		{
			//setFirstPosition();
			col = keys.get(0);
		}
		
		if (!doFirst)
		{
			if (equalVal == null)
			{
				this.setEqualsPos(val);
				//System.out.println("Starting with " + val);
			}
			else
			{
				this.setEqualsPos(equalVal);
				//System.out.println("Starting with " + equalVal);
			}
			
			calculateSecondaryTerminations();
			//System.out.println("Terminating conditions are " + terminates);
		}
		else
		{
			Object val2 = calculateSecondaryStarting();
			if (val2 == null)
			{
				this.setFirstPosition();
				//System.out.println("Starting at beginning of index");
			}
			else
			{
				this.setEqualsPos(val2);
				//System.out.println("Starting with " + val2);
			}
			
			calculateSecondaryTerminations();
			//System.out.println("Terminating conditions are " + terminates);
		}
	}
	
	private void switchOp()
	{
		if (op.equals("E") || op.equals("NE") || op.equals("LI") || op.equals("NL"))
		{
			return;
		}
		
		if (op.equals("L"))
		{
			op = "G";
			return;
		}
		
		if (op.equals("LE"))
		{
			op = "GE";
			return;
		}
		
		if (op.equals("G"))
		{
			op = "L";
			return;
		}
		
		if (op.equals("GE"))
		{
			op = "LE";
			return;
		}
	}
	
	private void setFirstPosition()
	{
		try
		{
			line = in.readLine();
			long off = -1;
			while (true)
			{
				FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
				int i = 0;
				while (i < keys.size())
				{
					tokens.nextToken();
					i++;
				}	
				off = Long.parseLong(tokens.nextToken());
				if (off >= 0)
				{
					break;
				}
				in.seek(-1 * off);
				line = in.readLine();
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
	}
	
	private String getPrefix(Object x)
	{
		return ((String)x).substring(0, ((String)x).indexOf("%"));
	}
	
	private String nextGT(String prefix)
	{
		String retval = prefix.substring(0, prefix.length() - 1);
		char x = prefix.charAt(prefix.length() - 1);
		x++;
		retval += x;
		return retval;
	}
	
	private void setEqualsPos(Object val)
	{
		int count = 0;
		try
		{
			line = in.readLine();
			count++;
			long oldOff = -1;
			int i = -1;
			FastStringTokenizer tokens = null;
			while (true)
			{
				while (!line.equals("\u0000"))
				{
					tokens = new FastStringTokenizer(line, "|", false);
					i = 0;
					//System.out.println("Col = " + col);
					while (i < keys.indexOf(col))
					{
						tokens.nextToken();
						i++;
					}	
					String keyVal = tokens.nextToken();
					i++;
				
					String type = types.get(keys.indexOf(col));
					Object key = null;
					try
					{
						key = getObject(keyVal, type);
					}
					catch(Exception e)
					{
						System.out.println("Error parsing value in Index.");
						System.out.println("Line is " + line);
						System.out.println("File is " + in.theFile.toString());
						System.exit(1);
					}
					if (orders.get(0))
					{
						if (((Comparable)key).compareTo(val) < 0)
						{
							while (i < keys.size())
							{
								tokens.nextToken();
								i++;
							}
						
							try
							{
								oldOff = Long.parseLong(tokens.nextToken());
							}
							catch(Exception e)
							{
								e.printStackTrace();
								System.out.println("Ran out of tokens reading line " + line);
								System.exit(1);
							}
						}
						else if (((Comparable)key).compareTo(val) == 0)
						{
							if (keys.size() == 1)
							{
								while (i < keys.size())
								{
									tokens.nextToken();
									i++;
								}
						
								oldOff = Long.parseLong(tokens.nextToken());
							}
							break;
						}
						else
						{
							break;
						}
					}
					else
					{
						if (((Comparable)key).compareTo(val) > 0)
						{
							while (i < keys.size())
							{
								tokens.nextToken();
								i++;
							}
						
							oldOff = Long.parseLong(tokens.nextToken());
						}
						else if (((Comparable)key).compareTo(val) == 0)
						{
							if (keys.size() == 1)
							{
								while (i < keys.size())
								{
									tokens.nextToken();
									i++;
								}
						
								oldOff = Long.parseLong(tokens.nextToken());
							}
							break;
						}
						else
						{
							break;
						}
					}
					
					line = in.readLine();
					count++;
				}
				
				if (oldOff == -1)
				{
					while (i < keys.size())
					{
						tokens.nextToken();
						i++;
					}
				
					oldOff = Long.parseLong(tokens.nextToken());
				}

				if (oldOff >= 0)
				{
					break;
				}
				in.seek(-1 * oldOff);
				line = in.readLine();
				oldOff = -1;
				count++;
				if (count > 500)
				{
					System.out.println(count + " rows have been read trying to find starting position based on " + val + " in index " + fileName);
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		//System.out.println("First line is " + line);
	}
	
	private Object getObject(String val, String type)
	{
		if (type.equals("INT"))
		{
			return ResourceManager.internLong(Long.parseLong(val));
		}
		else if (type.equals("FLOAT"))
		{
			return ResourceManager.internDouble(Double.parseDouble(val));
		}
		else if (type.equals("CHAR"))
		{
			return ResourceManager.internString(val);
		}
		else if (type.equals("LONG"))
		{
			return ResourceManager.internLong(Long.parseLong(val));
		}
		else if (type.equals("DATE"))
		{
			try
			{
				return DateParser.parse(val);
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(1);
			}
		}
		
		return null;
	}
	
	private boolean marksEnd()
	{
		if (line.equals("\u0000"))
		{
			return true;
		}
		
		if (terminates.size() == 0)
		{
			return false;
		}
		
		FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
		ArrayList<Object> row = new ArrayList<Object>(tokens.allTokens().length);
		for (String type : types)
		{
			row.add(getObject(tokens.nextToken(), type));
		}
		try
		{
			for (Filter terminate : terminates)
			{
				if (terminate.passes(row, cols2Pos))
				{
					return true;
				}
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	private ArrayListLong getRids()
	{
		FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
		int i = 0;
		while (i < keys.size())
		{
			tokens.nextToken();
			i++;
		}
		
		ArrayListLong retval = new ArrayListLong(tokens.allTokens().length);
		while (tokens.hasMoreTokens())
		{
			retval.add(Long.parseLong(tokens.nextToken()));
		}
		
		return retval;
	}
	
	private boolean currentKeySatisfies()
	{
		if (line.equals("\u0000"))
		{
			return false;
		}
		
		ArrayList<Object> row = new ArrayList<Object>(types.size());
		FastStringTokenizer tokens = new FastStringTokenizer(line, "|", false);
		for (String type : types)
		{
			row.add(getObject(tokens.nextToken(), type));
		}
		
		//System.out.println("Row = " + row);
		//System.out.println("Cols2Pos = " + cols2Pos);
		try
		{
			if (!f.passes(row, cols2Pos))
			{
				//System.out.println("Filter " + f + " returns false");
				return false;
			}
			else
			{
				//System.out.println("Filter " + f + " returns true");
			}
			
			for (Filter filter : secondary)
			{
				if (!filter.passes(row, cols2Pos))
				{
					//System.out.println("Filter " + filter + " returns false");
					return false;
				}
				else
				{
					//System.out.println("Filter " + filter + " returns false");
				}
			}
			
			return true;
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		return false;
	}
	
	public String toString()
	{
		return super.toString() + ": " + keys.toString();
	}
	
	public String getFileName()
	{
		return fileName;
	}
	
	public Filter getFilter()
	{
		return f;
	}
	
	public void addSecondaryFilter(Filter filter)
	{
		secondary.add(filter);
	}
	
	private Object calculateSecondaryStarting()
	{
		for (Filter filter : secondary)
		{
			String col2 = null;
			String op2 = filter.op();
			Object val = null;
			String orig = null;
			if (filter.leftIsColumn())
			{
				col2 = filter.leftColumn();
				val = filter.rightLiteral();
				orig = filter.rightOrig();
			}
			else
			{
				col2 = filter.rightColumn();
				val = filter.leftLiteral();
				orig = filter.leftOrig();
				if (op2.equals("L"))
				{
					op2 = "G";
				}
				else if (op2.equals("LE"))
				{
					op2 = "GE";
				}
				else if (op2.equals("G"))
				{
					op2 = "L";
				}
				else if (op2.equals("GE"))
				{
					op2 = "LE";
				}
			}
			
			if (keys.get(0).equals(col2) && (!filter.leftIsColumn() || !filter.rightIsColumn()))
			{
				try
				{
					if (op2.equals("L"))
					{
						if (orders.get(0))
						{
							//setFirstPosition();
							//terminate = new Filter(col, "GE", orig);
						}
						else
						{
							return val;
						}
					}
					else if (op2.equals("LE"))
					{
						if (orders.get(0))
						{
							//setFirstPosition();
							//terminate = new Filter(col, "G", orig);
						}
						else
						{
							return val;
						}
					}
					else if (op2.equals("G"))
					{
						if (orders.get(0))
						{
							return val;
						}
						else
						{
							//setFirstPosition();
							//terminate = new Filter(col, "LE", orig);
						}
					}
					else if (op2.equals("GE"))
					{
						if (orders.get(0))
						{
							return val;
						}
						else
						{
							//setFirstPosition();
							//terminate = new Filter(col, "L", orig);
						}
					}
					else if (op2.equals("E"))
					{
						return val;
					}
					else if (op2.equals("NE"))
					{
						//setFirstPosition();
					}
					else if (op2.equals("LI"))
					{
						String prefix = getPrefix(val);
						if (prefix.length() > 0)
						{
							if (orders.get(0))
							{
								return prefix;
							}
							else
							{
								return nextGT(prefix);
							}
						}
						else
						{
							//setFirstPosition();
						}
					}
					else if (op2.equals("NL"))
					{
						//setFirstPosition();
					}
					else
					{
						System.out.println("Unknown operator in Index: " + op);
						System.exit(1);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			else
			{
				//setFirstPosition();
			}
		}
		
		return null;
	}
	
	private void calculateSecondaryTerminations()
	{
		for (Filter filter : secondary)
		{
			String col2 = null;
			String op2 = filter.op();
			Object val = null;
			String orig = null;
			if (filter.leftIsColumn())
			{
				col2 = filter.leftColumn();
				val = filter.rightLiteral();
				orig = filter.rightOrig();
			}
			else
			{
				col2 = filter.rightColumn();
				val = filter.leftLiteral();
				orig = filter.leftOrig();
				if (op2.equals("L"))
				{
					op2 = "G";
				}
				else if (op2.equals("LE"))
				{
					op2 = "GE";
				}
				else if (op2.equals("G"))
				{
					op2 = "L";
				}
				else if (op2.equals("GE"))
				{
					op2 = "LE";
				}
			}
			
			if (keys.get(0).equals(col2)  && (!filter.leftIsColumn() || !filter.rightIsColumn()))
			{
				try
				{
					if (op2.equals("L"))
					{
						if (orders.get(0))
						{
							terminates.add(new Filter(col2, "GE", orig));
						}
						else
						{
							//setEqualsPos(val);
							//doFirst = false;
						}
					}
					else if (op2.equals("LE"))
					{
						if (orders.get(0))
						{
							//setFirstPosition();
							terminates.add(new Filter(col2, "G", orig));
						}
						else
						{
							//setEqualsPos(val);
							//doFirst = false;
						}
					}
					else if (op2.equals("G"))
					{
						if (orders.get(0))
						{
							//setEqualsPos(val);
							//doFirst = false;
						}
						else
						{
							//setFirstPosition();
							terminates.add(new Filter(col2, "LE", orig));
						}
					}
					else if (op2.equals("GE"))
					{
						if (orders.get(0))
						{
							//setEqualsPos(val);
							//doFirst = false;
						}
						else
						{
							//setFirstPosition();
							terminates.add(new Filter(col2, "L", orig));
						}
					}
					else if (op2.equals("E"))
					{
						//setEqualsPos(val);
						//doFirst = false;
						terminates.add(new Filter(col2, "NE", orig));
					}
					else if (op2.equals("NE"))
					{
						//setFirstPosition();
					}
					else if (op2.equals("LI"))
					{
						String prefix = getPrefix(val);
						if (prefix.length() > 0)
						{
							if (orders.get(0))
							{
								//setEqualsPos(prefix);
								//doFirst = false;
								//equalVal = prefix;
								terminates.add(new Filter(col2, "GE", "'" + nextGT(prefix) + "'"));
							}
							else
							{
								//setEqualsPos(nextGT(prefix));
								//doFirst = false;
								//equalVal = nextGT(prefix);
								terminates.add(new Filter(col2, "L", "'" + prefix + "'"));
							}
						}
						else
						{
							//setFirstPosition();
						}
					}
					else if (op.equals("NL"))
					{
						//setFirstPosition();
					}
					else
					{
						System.out.println("Unknown operator in Index: " + op);
						System.exit(1);
					}
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
			else
			{
				//setFirstPosition();
			}
		}
	}
	
	public ArrayList<String> getReferencedCols()
	{
		ArrayList<String> retval = new ArrayList<String>();
		if (f.leftIsColumn())
		{
			retval.add(f.leftColumn());
		}
		
		if (f.rightIsColumn())
		{
			if (!retval.contains(f.rightColumn()))
			{
				retval.add(f.rightColumn());
			}
		}
		
		for (Filter filter : secondary)
		{		
			if (filter.leftIsColumn())
			{
				if (!retval.contains(filter.leftColumn()))
				{
					retval.add(filter.leftColumn());
				}
			}
			
			if (filter.rightIsColumn())
			{
				if (!retval.contains(filter.rightColumn()))
				{
					retval.add(filter.rightColumn());
				}
			}
		}
		
		return retval;
	}
	
	public ArrayList<String> getCols()
	{
		return keys;
	}
	
	public ArrayList<Filter> getSecondary()
	{
		return secondary;
	}
	
	public ArrayList<String> setIndexOnly(ArrayList<String> references, ArrayList<String> types)
	{
		System.out.println("References = " + references);
		System.out.println("Types = " + types);
		indexOnly = true;
		ArrayList<String> retval = new ArrayList<String>(keys.size());
		int i = 0;
		for (String col : keys)
		{
			if (references.contains(col))
			{
				fetches.add(i);
				fetchTypes.add(types.get(references.indexOf(col)));
				retval.add(col);
			}
			
			i++;
		}
		
		return retval;
	}
	
	public void reset()
	{
		try
		{
			in.seek(0);
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.exit(1);
		}
		
		delayed = true;
		positioned = false;
		terminates = new ArrayList<Filter>();
		
		if (delayedConditions != null)
		{
			for (Filter filter : delayedConditions)
			{
				if (f != null && filter.equals(f))
				{
					f = null;
				}
				else
				{
					secondary.remove(filter);
				}
			}
			delayedConditions = null;
		}
	}
	
	public void runDelayed()
	{
		delayed = true;
	}
}

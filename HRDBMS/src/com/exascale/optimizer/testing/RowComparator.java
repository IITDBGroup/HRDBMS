package com.exascale.optimizer.testing;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;

public class RowComparator implements Comparator 
{

	protected ArrayList<Boolean> orders;
	protected ArrayList<String> types;
	//protected SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	
	public RowComparator(ArrayList<Boolean> orders, ArrayList<String> types)
	{
		this.orders = orders;
		this.types = types;
	}
	
	@Override
	public int compare(Object arg0, Object arg1) 
	{
		int result;
		
		ArrayList<Object> lhs = new ArrayList<Object>(types.size());
		int i = 0;
		while (i <  types.size())
		{
			String type = types.get(i);
			if (type.equals("INT"))
			{
				lhs.add(ResourceManager.internInt(Integer.parseInt(((String[])arg0)[i])));
			}
			else if (type.equals("FLOAT"))
			{
				lhs.add(ResourceManager.internDouble(Double.parseDouble(((String[])arg0)[i])));
			}
			else if (type.equals("CHAR"))
			{
				lhs.add(ResourceManager.internString(((String[])arg0)[i]));
			}
			else if (type.equals("LONG"))
			{
				lhs.add(ResourceManager.internLong(Long.parseLong(((String[])arg0)[i])));
			}
			else if (type.equals("DATE"))
			{
				try
				{
					lhs.add(DateParser.parse(((String[])arg0)[i]));
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
				
			i++;
		}
		
		ArrayList<Object> rhs = new ArrayList<Object>(types.size());
		i = 0;
		while (i <  types.size())
		{
			String type = types.get(i);
			if (type.equals("INT"))
			{
				rhs.add(ResourceManager.internInt(Integer.parseInt(((String[])arg1)[i])));
			}
			else if (type.equals("FLOAT"))
			{
				rhs.add(ResourceManager.internDouble(Double.parseDouble(((String[])arg1)[i])));
			}
			else if (type.equals("CHAR"))
			{
				rhs.add(ResourceManager.internString(((String[])arg1)[i]));
			}
			else if (type.equals("LONG"))
			{
				rhs.add(ResourceManager.internLong(Long.parseLong(((String[])arg1)[i])));
			}
			else if (type.equals("DATE"))
			{
				try
				{
					rhs.add(DateParser.parse(((String[])arg1)[i]));
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
				
			i++;
		}
		
		i = 0;
		while (i < types.size())
		{
			Object lField = lhs.get(i);
			Object rField = rhs.get(i);
			
			if (lField instanceof Integer)
			{
				result = ((Integer)lField).compareTo((Integer)rField);
			}
			else if (lField instanceof Long)
			{
				result = ((Long)lField).compareTo((Long)rField);
			}
			else if (lField instanceof Double)
			{
				result = ((Double)lField).compareTo((Double)rField);
			}
			else if (lField instanceof String)
			{
				result = ((String)lField).compareTo((String)rField);
			}
			else if (lField instanceof Date)
			{
				result = ((Date)lField).compareTo((Date)rField);
			}
			else
			{
				System.out.println("Unknown type in SortOperator.compare(): " + lField.getClass());
				System.exit(1);
				result = 0;
			}
			
			if (orders.get(i))
			{
				if (result > 0)
				{
					return 1;
				}
				else if (result < 0)
				{
					return -1;
				}
			}
			else
			{
				if (result > 0)
				{
					return -1;
				}
				else if (result < 0)
				{
					return 1;
				}
			}
			
			i++;
		}
		
		return 0;
	}
}

package com.exascale.misc;

import java.util.ArrayList;
import java.util.Comparator;
import com.exascale.managers.HRDBMSWorker;

public final class RowComparator implements Comparator
{

	private final ArrayList<Boolean> orders;
	private final ArrayList<String> types;

	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");

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
		ArrayList<Object> rhs = new ArrayList<Object>(types.size());
		int i = 0;
		
		if (arg0 instanceof ArrayList)
		{
			lhs = (ArrayList<Object>)arg0;
		}
		else
		{
			while (i < types.size())
			{
				final String type = types.get(i);
				if (type.equals("INT"))
				{
					lhs.add(Utils.parseInt(((String[])arg0)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					lhs.add(Utils.parseDouble(((String[])arg0)[i]));
				}
				else if (type.equals("CHAR"))
				{
					lhs.add(((String[])arg0)[i]);
				}
				else if (type.equals("LONG"))
				{
					lhs.add(Utils.parseLong(((String[])arg0)[i]));
				}
				else if (type.equals("DATE"))
				{
					try
					{
						lhs.add(DateParser.parse(((String[])arg0)[i]));
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
					}
				}

				i++;
			}
		}
		
		if (arg1 instanceof ArrayList)
		{
			rhs = (ArrayList<Object>)arg1;
		}
		else
		{
			i = 0;
			while (i < types.size())
			{
				final String type = types.get(i);
				if (type.equals("INT"))
				{
					rhs.add(Utils.parseInt(((String[])arg1)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					rhs.add(Utils.parseDouble(((String[])arg1)[i]));
				}
				else if (type.equals("CHAR"))
				{
					rhs.add(((String[])arg1)[i]);
				}
				else if (type.equals("LONG"))
				{
					rhs.add(Utils.parseLong(((String[])arg1)[i]));
				}
				else if (type.equals("DATE"))
				{
					try
					{
						rhs.add(DateParser.parse(((String[])arg1)[i]));
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						System.exit(1);
					}
				}

				i++;
			}
		}

		i = 0;
		while (i < types.size())
		{
			final Object lField = lhs.get(i);
			final Object rField = rhs.get(i);

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
			else if (lField instanceof MyDate)
			{
				result = ((MyDate)lField).compareTo(rField);
			}
			else
			{
				HRDBMSWorker.logger.error("Unknown type in SortOperator.compare(): " + lField.getClass());
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

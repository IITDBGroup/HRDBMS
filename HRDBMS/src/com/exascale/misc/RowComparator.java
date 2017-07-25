package com.exascale.misc;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import com.exascale.managers.HRDBMSWorker;

public final class RowComparator implements Comparator
{

	private final boolean[] orders;
	private final String[] types;

	// private MySimpleDateFormat sdf = new MySimpleDateFormat("yyyy-MM-dd");

	public RowComparator(final List<Boolean> orders, final List<String> types)
	{
		this.orders = new boolean[orders.size()];
		this.types = new String[types.size()];
		int i = 0;
		while (i < orders.size())
		{
			this.orders[i] = orders.get(i);
			this.types[i] = types.get(i);
			i++;
		}
	}

	@Override
	public int compare(final Object arg0, final Object arg1)
	{
		int result;

		final int size = types.length;
		List<Object> lhs = null;
		List<Object> rhs = null;
		int i = 0;

		if (arg0 instanceof ArrayList)
		{
			lhs = (List<Object>)arg0;
		}
		else
		{
			lhs = new ArrayList<Object>(size);
			while (i < size)
			{
				final String type = types[i];
				if (type.equals("INT"))
				{
					lhs.add(Utils.parseInt(((String[])arg0)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					lhs.add(Double.parseDouble(((String[])arg0)[i]));
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
			rhs = (List<Object>)arg1;
		}
		else
		{
			rhs = new ArrayList<Object>(size);
			i = 0;
			while (i < size)
			{
				final String type = types[i];
				if (type.equals("INT"))
				{
					rhs.add(Utils.parseInt(((String[])arg1)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					rhs.add(Double.parseDouble(((String[])arg1)[i]));
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
		while (i < size)
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

			if (orders[i])
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

	public int compare(final Object arg0, final Object arg1, final boolean debug)
	{
		int result;

		final int size = types.length;
		HRDBMSWorker.logger.debug("Type is " + types);
		List<Object> lhs = null;
		List<Object> rhs = null;
		int i = 0;

		if (arg0 instanceof ArrayList)
		{
			lhs = (List<Object>)arg0;
		}
		else
		{
			lhs = new ArrayList<Object>(size);
			while (i < size)
			{
				final String type = types[i];
				if (type.equals("INT"))
				{
					lhs.add(Utils.parseInt(((String[])arg0)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					lhs.add(Double.parseDouble(((String[])arg0)[i]));
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
			rhs = (List<Object>)arg1;
		}
		else
		{
			rhs = new ArrayList<Object>(size);
			i = 0;
			while (i < size)
			{
				final String type = types[i];
				if (type.equals("INT"))
				{
					rhs.add(Utils.parseInt(((String[])arg1)[i]));
				}
				else if (type.equals("FLOAT"))
				{
					rhs.add(Double.parseDouble(((String[])arg1)[i]));
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

		HRDBMSWorker.logger.debug("LHS is " + lhs);
		HRDBMSWorker.logger.debug("RHS is " + rhs);
		i = 0;
		while (i < size)
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

			if (orders[i])
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

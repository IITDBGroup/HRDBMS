package com.exascale.misc;

import com.exascale.managers.HRDBMSWorker;
import com.exascale.optimizer.MetaData;
import com.exascale.optimizer.Operator;
import com.exascale.tables.Transaction;

import java.util.ArrayList;

public final class Utils
{
	public static ArrayList<Object> convertToHosts(final ArrayList<Object> tree, final Transaction tx) throws Exception
	{
		final ArrayList<Object> retval = new ArrayList<Object>();
		int i = 0;
		final int size = tree.size();
		while (i < size)
		{
			final Object obj = tree.get(i);
			if (obj instanceof Integer)
			{
				// new MetaData();
				retval.add(MetaData.getHostNameForNode((Integer)obj, tx));
			}
			else
			{
				retval.add(convertToHosts((ArrayList<Object>)obj, tx));
			}

			i++;
		}

		return retval;
	}

	/** Recurses through operator tree and prints for debugging purposes at the TRACE level */
	public static void printTree(final Operator op, final int indent)
	{
		if(!HRDBMSWorker.logger.isTraceEnabled()) {
			return;
		}
		StringBuilder line = new StringBuilder();
		for (int i = 0; i < indent; i++)
		{
			line.append(" ");
		}
		line.append(op).append("[").append(System.identityHashCode(op)).append("]");

		try {
			Object p = op.parent();
			line.append(" parent:").append(p == null ? "" : System.identityHashCode(op.parent()));
		} catch(UnsupportedOperationException e) {}

		HRDBMSWorker.logger.trace(line);

		if (!op.children().isEmpty())
		{
			line = new StringBuilder();
			for (int i = 0; i < indent; i++)
			{
				line.append(" ");
			}
			line.append("(");
			HRDBMSWorker.logger.trace(line);

			for (final Operator child : op.children())
			{
				printTree(child, indent + 3);
			}

			line = new StringBuilder();
			for (int i = 0; i < indent; i++)
			{
				line.append(" ");
			}
			line.append(")");
			HRDBMSWorker.logger.trace(line);
		}
	}

	public static final double parseDouble(final String s)
	{
		final int p = s.indexOf('.');
		if (p < 0)
		{
			return parseLong(s);
		}

		boolean negative = false;
		int offset = 0;
		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}

		while (s.charAt(offset) == '0')
		{
			offset++;
		}

		final String s2 = s.substring(offset, p) + s.substring(p + 1, s.length());
		final long n = parseLong(s2);
		final int x = s.length() - p - 1;
		int i = 0;
		long d = 1;
		while (i < x)
		{
			d *= 10;
			i++;
		}

		double retval = (n * 1.0) / (d * 1.0);
		if (negative)
		{
			retval *= -1;
		}

		return retval;
	}

	public static final int parseInt(final String s)
	{
		boolean negative = false;
		int offset = 0;
		int result = 0;
		final int length = s.length();

		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}

		while (offset < length)
		{
			byte b = (byte)s.charAt(offset);
			b -= 48;
			result *= 10;
			result += b;
			offset++;
		}

		if (negative)
		{
			result *= -1;
		}

		return result;
	}

	public static final long parseLong(final String s)
	{
		boolean negative = false;
		int offset = 0;
		long result = 0;
		final int length = s.length();

		if (s.charAt(0) == '-')
		{
			negative = true;
			offset = 1;
		}

		while (offset < length)
		{
			byte b = (byte)s.charAt(offset);
			b -= 48;
			result *= 10;
			result += b;
			offset++;
		}

		if (negative)
		{
			result *= -1;
		}

		return result;
	}
}

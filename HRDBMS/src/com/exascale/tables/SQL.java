package com.exascale.tables;

public class SQL
{
	private String internal;

	public SQL(final String sql)
	{
		internal = toUpperCaseExceptQuoted(sql);
		internal = removeExcessWhitespace(internal);
	}

	private static String removeExcessWhitespace(final String in)
	{
		String out = in.replace('\t', ' ');
		out = out.replace('\n', ' ');
		out = out.replace('\r', ' ');
		out = out.replace('\f', ' ');
		int i = 0;
		int whitespaceCount = 0;
		final StringBuilder out2 = new StringBuilder();
		final int length = out.length();
		while (i < length)
		{
			if (out.charAt(i) != ' ')
			{
				whitespaceCount = 0;
				out2.append(out.charAt(i));
			}
			else
			{
				if (whitespaceCount == 0)
				{
					out2.append(' ');
					whitespaceCount = 1;
				}
				else
				{
				}
			}

			i++;
		}

		return out2.toString();
	}

	private static String toUpperCaseExceptQuoted(final String in)
	{
		final StringBuilder out = new StringBuilder();
		int i = 0;
		boolean quoted = false;
		int quoteType = 0;
		final int length = in.length();
		while (i < length)
		{
			if ((in.charAt(i) != '\'' && in.charAt(i) != '"') || (in.charAt(i) == '\'' && quoteType == 2) || (in.charAt(i) == '"' && quoteType == 1))
			{
				if (!quoted)
				{
					out.append(Character.toUpperCase(in.charAt(i)));
				}
				else
				{
					out.append(in.charAt(i));
				}
			}
			else
			{
				if (quoteType == 0)
				{
					if (in.charAt(i) == '\'' && ((i + 1) == in.length() || in.charAt(i + 1) != '\''))
					{
						quoteType = 1;
						quoted = true;
						out.append('\'');
					}
					else if (in.charAt(i) == '"' && ((i + 1) == in.length() || in.charAt(i + 1) != '"'))
					{
						quoteType = 2;
						quoted = true;
						out.append('"');
					}
					else
					{
						out.append(in.charAt(i));
						out.append(in.charAt(i + 1));
						i++;
					}
				}
				else if (quoteType == 1)
				{
					if (in.charAt(i) == '\'' && ((i + 1) == in.length() || in.charAt(i + 1) != '\''))
					{
						quoteType = 0;
						quoted = false;
						out.append('\'');
					}
					else if (in.charAt(i) == '"')
					{
						out.append('"');
					}
					else
					{
						out.append("\'\'");
						i++;
					}
				}
				else
				{
					if (in.charAt(i) == '"' && ((i + 1) == in.length() || in.charAt(i + 1) != '"'))
					{
						quoteType = 0;
						quoted = false;
						out.append('"');
					}
					else if (in.charAt(i) == '\'')
					{
						out.append('\'');
					}
					else
					{
						out.append("\"\"");
						i++;
					}
				}
			}

			i++;
		}

		return out.toString();
	}

	@Override
	public boolean equals(final Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}

		if (rhs instanceof SQL)
		{
			final SQL r = (SQL)rhs;
			return internal.equals(r.internal);
		}

		return false;
	}

	@Override
	public int hashCode()
	{
		return internal.hashCode();
	}

	@Override
	public String toString()
	{
		return internal;
	}
}

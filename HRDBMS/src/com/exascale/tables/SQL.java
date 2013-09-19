package com.exascale.tables;

public class SQL 
{
	private String internal;
	
	public SQL(String sql)
	{
		internal = toUpperCaseExceptQuoted(sql);
		internal = removeExcessWhitespace(internal);
	}
	
	private String removeExcessWhitespace(String in)
	{
		String out = in.replace('\t', ' ');
		out = out.replace('\n', ' ');
		out = out.replace('\r', ' ');
		out = out.replace('\f', ' ');
		int i = 0;
		int whitespaceCount = 0;
		String out2 = "";
		while (i < out.length())
		{
			if (out.charAt(i) != ' ')
			{
				whitespaceCount = 0;
				out2 += out.charAt(i);
			}
			else
			{
				if (whitespaceCount == 0)
				{
					out2 += ' ';
					whitespaceCount = 1;
				}
				else
				{}
			}
			
			i++;
		}
		
		return out2;
	}
	
	public boolean equals(Object rhs)
	{
		if (rhs == null)
		{
			return false;
		}
		
		if (rhs instanceof SQL)
		{
			SQL r = (SQL)rhs;
			return internal.equals(r.internal);
		}
		
		return false;
	}
	
	public int hashCode()
	{
		return internal.hashCode();
	}
	
	private String toUpperCaseExceptQuoted(String in)
	{
		String out = "";
		int i = 0;
		boolean quoted = false;
		int quoteType = 0;
		while (i < in.length())
		{
			if ((in.charAt(i) != '\'' && in.charAt(i) != '"') || (in.charAt(i) == '\'' && quoteType == 2) || (in.charAt(i) == '"' && quoteType == 1))
			{
				if (!quoted)
				{
					out += Character.toUpperCase(in.charAt(i));
				}
				else
				{
					out += in.charAt(i);
				}
			}
			else
			{
				if (quoteType == 0)
				{
					if (in.charAt(i) == '\'' && ((i+1) == in.length() || in.charAt(i+1) != '\''))
					{
						quoteType = 1;
						quoted = true;
						out += '\'';
					}
					else if (in.charAt(i) == '"' && ((i+1) == in.length() || in.charAt(i+1) != '"'))
					{
						quoteType = 2;
						quoted = true;
						out += '"';
					}
					else
					{
						out += in.charAt(i);
						out += in.charAt(i+1);
						i++;
					}
				}
				else if (quoteType == 1)
				{
					if (in.charAt(i) == '\'' && ((i+1) == in.length() || in.charAt(i+1) != '\''))
					{
						quoteType = 0;
						quoted = false;
						out += '\'';
					}
					else if (in.charAt(i) == '"')
					{
						out += '"';
					}
					else
					{
						out += "\'\'";
						i++;
					}
				}
				else 
				{
					if (in.charAt(i) == '"' && ((i+1) == in.length() || in.charAt(i+1) != '"'))
					{
						quoteType = 0;
						quoted = false;
						out += '"';
					}
					else if (in.charAt(i) == '\'')
					{
						out += '\'';
					}
					else
					{
						out += "\"\"";
						i++;
					}
				}
			}
			
			i++;
		}
		
		return out;
	}
}

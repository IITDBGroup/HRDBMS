package com.exascale.optimizer.testing;

import java.io.Serializable;

public final class FastStringTokenizer implements Serializable {
    protected String[] result;
    protected int index;
    protected String delim;
    protected String string;
    protected String[] temp;
    
    public final void reuse(String string, String delim, boolean bool)
    {
    	this.delim = delim;
    	this.string = string;
    	char delimiter = delim.charAt(0);
    	
    	if (temp.length < string.length() / 2 + 1)
    	{
    		temp = new String[string.length() / 2 + 1];
    	}
    	
    	int wordCount = 0;
    	int i = 0;
    	int j = string.indexOf(delimiter);
    	
    	while (j >= 0)
        {
            temp[wordCount++] = string.substring(i, j);
            i = j + 1;
            j = string.indexOf(delimiter, i);
        }
 
        if (i < string.length())
        {
        	temp[wordCount++] = string.substring(i);
        }
        
        if (result.length != wordCount)
        {
        	result = new String[wordCount];
        }
        
        System.arraycopy(temp, 0, result, 0, wordCount);
        index = 0;
    }
    
    public FastStringTokenizer clone()
    {
    	return new FastStringTokenizer(string, delim, false);
    }
    
    public FastStringTokenizer(String string, String delim, boolean bool)
    {
    	this.delim = delim;
    	this.string = string;
    	char delimiter = delim.charAt(0);
        
    	temp = new String[string.length() / 2 + 1];
        int wordCount = 0;
        int i = 0;
        int j = string.indexOf(delimiter);
 
        while (j >= 0)
        {
            temp[wordCount++] = string.substring(i, j);
            i = j + 1;
            j = string.indexOf(delimiter, i);
        }
 
        if (i < string.length())
        {
        	temp[wordCount++] = string.substring(i);
        }
 
        result = new String[wordCount];
        System.arraycopy(temp, 0, result, 0, wordCount);
        index = 0;
    }
    
    public boolean hasMoreTokens()
    {
    	return index < result.length;
    }
    
    public String nextToken()
    {
    	return result[index++];
    }
    
    public String[] allTokens()
    {
    	return result;
    }
    
    public void setIndex(int index)
    {
    	this.index = index;
    }
}
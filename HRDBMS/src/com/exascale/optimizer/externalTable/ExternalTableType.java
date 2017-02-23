package com.exascale.optimizer.externalTable;

import java.util.ArrayList;
import java.util.Properties;

public interface ExternalTableType
{
	public void initialize(Properties params);
	public ArrayList next();
	public boolean hasNext();
	public void reset(); //later
	// get schema info
	// ...
}

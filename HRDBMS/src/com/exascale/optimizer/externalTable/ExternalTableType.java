package com.exascale.optimizer.externalTable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.TreeMap;

public interface ExternalTableType
{
	void setParameters(Properties params);
	Properties getParameters();
	void start();
	ArrayList next();
	void close();
	void reset(); //later
	void setCols2Types(HashMap<String, String> cols2Types);
	void setCols2Pos(HashMap<String, Integer> cols2Pos);
	void setPos2Col(TreeMap<Integer, String> pos2Col);
	void setName(String name);
	void setSchema(String schema);

}

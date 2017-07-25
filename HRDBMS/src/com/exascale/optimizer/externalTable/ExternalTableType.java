package com.exascale.optimizer.externalTable;

import java.io.OutputStream;
import java.util.*;

/** Defines an implementation of a specific type of external table */
public interface ExternalTableType
{
	/** Sets the passed properties to help set up the external table implementation */
	void setParams(ExternalParamsInterface params);
	Class<ExternalParamsInterface> getParamsClass() throws NoSuchFieldException;
	/** Returns the next result from the external table */
	List<?> next();
	boolean hasNext();
	/** Resets retrieval from the external table back to the beginning of the table */
	void reset();
	void start();
	void close();
	void setCols2Types(Map<String, String> cols2Types);
	void setCols2Pos(Map<String, Integer> cols2Pos);
	void setPos2Col(Map<Integer, String> pos2Col);
	void setName(String name);
	void setSchema(String schema);
    void serialize(final OutputStream out, final IdentityHashMap<Object, Long> prev) throws Exception;
}
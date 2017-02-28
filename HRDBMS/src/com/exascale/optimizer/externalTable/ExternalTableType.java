package com.exascale.optimizer.externalTable;

import java.util.List;

/** Defines an implementation of a specific type of external table */
public interface ExternalTableType
{
	/** Sets the passed properties to help set up the external table implementation */
	void initialize(String params);
	/** Returns the next result from the external table */
	List<?> next();
	boolean hasNext();
	/** Resets retrieval from the external table back to the beginning of the table */
	void reset();
}

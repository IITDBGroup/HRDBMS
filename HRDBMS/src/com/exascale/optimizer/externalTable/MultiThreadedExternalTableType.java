package com.exascale.optimizer.externalTable;

/** Defines an implementation of a specific type of external table to read data in multiple threads  */
public interface MultiThreadedExternalTableType extends ExternalTableType
{
	MultiThreadedExternalTableType clone();
	void setDevice(int device);
    void setNode(int node);
    void setNumNodes(int numNodes);
    void setNumDevices(int numDevices);
}
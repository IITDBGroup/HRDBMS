package com.exascale.optimizer.load;

import com.exascale.optimizer.PartitionMetaData;

import java.io.Serializable;
import java.util.*;

public class LoadMetaData implements Serializable
{
	// numNodes = (Integer)objIn.readObject();
	// delimiter = (String)objIn.readObject();
	// pos2Col = (Map<Integer, String>)objIn.readObject();
	// cols2Types = (Map<String, String>)objIn.readObject();
	// pos2Length = (Map<Integer, Integer>)objIn.readObject();
	// pmd = (PartitionMetaData)objIn.readObject();
	public int numNodes;
	public String delimiter;
	public Map<Integer, String> pos2Col;
	public Map<String, String> cols2Types;
	public Map<Integer, Integer> pos2Length;
	public PartitionMetaData pmd;
	public List<Integer> workerNodes;
	public List<Integer> coordNodes;
	public long txNum;
	public List<List<String>> keys;
	public List<List<String>> types;
	public List<List<Boolean>> orders;
	public List<String> indexes;
	public int type;

	public LoadMetaData(final int numNodes, final String delimiter, final Map<Integer, String> pos2Col, final Map<String, String> cols2Types, final Map<Integer, Integer> pos2Length, final PartitionMetaData pmd, final List<Integer> workerNodes, final List<Integer> coordNodes, final long txNum, final List<List<String>> keys, final List<List<String>> types, final List<List<Boolean>> orders, final List<String> indexes, final int type)
	{
		this.numNodes = numNodes;
		this.delimiter = delimiter;
		this.pos2Col = pos2Col;
		this.cols2Types = cols2Types;
		this.pos2Length = pos2Length;
		this.pmd = pmd;
		this.workerNodes = workerNodes;
		this.coordNodes = coordNodes;
		this.txNum = txNum;
		this.keys = keys;
		this.types = types;
		this.orders = orders;
		this.indexes = indexes;
		this.type = type;
	}
}

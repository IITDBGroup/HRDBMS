package com.exascale.optimizer.load;

import com.exascale.optimizer.PartitionMetaData;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;

public class LoadMetaData implements Serializable
{
	// numNodes = (Integer)objIn.readObject();
	// delimiter = (String)objIn.readObject();
	// pos2Col = (TreeMap<Integer, String>)objIn.readObject();
	// cols2Types = (HashMap<String, String>)objIn.readObject();
	// pos2Length = (HashMap<Integer, Integer>)objIn.readObject();
	// pmd = (PartitionMetaData)objIn.readObject();
	public int numNodes;
	public String delimiter;
	public TreeMap<Integer, String> pos2Col;
	public HashMap<String, String> cols2Types;
	public HashMap<Integer, Integer> pos2Length;
	public PartitionMetaData pmd;
	public ArrayList<Integer> workerNodes;
	public ArrayList<Integer> coordNodes;
	public long txNum;
	public ArrayList<ArrayList<String>> keys;
	public ArrayList<ArrayList<String>> types;
	public ArrayList<ArrayList<Boolean>> orders;
	public ArrayList<String> indexes;
	public int type;

	public LoadMetaData(final int numNodes, final String delimiter, final TreeMap<Integer, String> pos2Col, final HashMap<String, String> cols2Types, final HashMap<Integer, Integer> pos2Length, final PartitionMetaData pmd, final ArrayList<Integer> workerNodes, final ArrayList<Integer> coordNodes, final long txNum, final ArrayList<ArrayList<String>> keys, final ArrayList<ArrayList<String>> types, final ArrayList<ArrayList<Boolean>> orders, final ArrayList<String> indexes, final int type)
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

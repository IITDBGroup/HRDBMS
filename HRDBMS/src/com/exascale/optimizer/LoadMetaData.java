package com.exascale.optimizer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import com.exascale.optimizer.MetaData.PartitionMetaData;

public class LoadMetaData implements Serializable
{
	//numNodes = (Integer)objIn.readObject();
	//delimiter = (String)objIn.readObject();
	//pos2Col = (TreeMap<Integer, String>)objIn.readObject();
	//cols2Types = (HashMap<String, String>)objIn.readObject();
	//pos2Length = (HashMap<Integer, Integer>)objIn.readObject();
	//pmd = (PartitionMetaData)objIn.readObject();
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
	
	public LoadMetaData(int numNodes, String delimiter, TreeMap<Integer, String> pos2Col, HashMap<String, String> cols2Types, HashMap<Integer, Integer> pos2Length, PartitionMetaData pmd, ArrayList<Integer> workerNodes, ArrayList<Integer> coordNodes, long txNum, ArrayList<ArrayList<String>> keys, ArrayList<ArrayList<String>> types, ArrayList<ArrayList<Boolean>> orders, ArrayList<String> indexes)
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
	}
}

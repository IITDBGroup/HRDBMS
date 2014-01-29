package com.exascale.optimizer.testing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.ArrayList;

public class Phase2 
{
	protected RootOperator root;
	protected MetaData meta;
	
	public Phase2(RootOperator root)
	{
		this.root = root;
		meta = root.getMeta();
	}
	
	public void optimize()
	{
		setPartitionMetaData(root);
		updateTree(root);
	}
	
	protected void setPartitionMetaData(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			TableScanOperator t = (TableScanOperator)op;
			if (!t.metaDataSet())
			{
				t.setMetaData();
			}
		}
		else
		{
			for (Operator o : op.children())
			{
				setPartitionMetaData(o);
			}
		}
	}
	
	protected void updateTree(Operator op)
	{
		if (op instanceof TableScanOperator)
		{
			TableScanOperator t = (TableScanOperator)op;
			if (t.phase2Done())
			{
				return;
			}
			
			t.setPhase2Done();
			for (Operator o : t.parents())
			{
				CNFFilter filter = t.getCNFForParent(o);
				if (t.noNodeGroupSet())
				{
					if (t.anyNode())
					{
						int i = Math.abs(new Random(System.currentTimeMillis()).nextInt());
						i = i % meta.getNumNodes();
						t.addActiveNodeForParent(i, o);
						setActiveDevices(t, filter, o);
					}
					else
					{
						//node is hash or range
						//node is set or ALL
						ArrayList<Integer> nodeList = new ArrayList<Integer>(meta.getNumNodes());
						int i = 0;
						while (i < meta.getNumNodes())
						{
							nodeList.add(i);
							i++;
						}
						
						ArrayList<ArrayList<Integer>> nodeLists = new ArrayList<ArrayList<Integer>>(1);
						nodeLists.add(nodeList);
						setActiveNodes(t, filter, o, nodeLists);
						setActiveDevices(t, filter, o);
					}
				}
				else
				{
					HashMap<Integer, ArrayList<Integer>> nodeGroupHashMap = t.getNodeGroupHashMap();
					ArrayList<Integer> entries = determineHashMapEntries(t, filter);
					ArrayList<ArrayList<Integer>> nodeLists = new ArrayList<ArrayList<Integer>>(entries.size());
					for (int entry : entries)
					{
						nodeLists.add(nodeGroupHashMap.get(entry));
					}
					setActiveNodes(t, filter, o, nodeLists);
					setActiveDevices(t, filter, o);
				}
			}
			
			for (Operator o : t.parents())
			{
				//get rid of duplicates in node and device lists
				HashSet hs = new HashSet();
				ArrayList<Integer> nodeList = t.getNodeList(o);
				hs.addAll(nodeList);
				nodeList.clear();
				nodeList.addAll(hs);
				hs.clear();
				ArrayList<Integer> deviceList = t.getDeviceList(o);
				hs.addAll(deviceList);
				deviceList.clear();
				deviceList.addAll(hs);
			}
			
			ArrayList<Operator> parents = (ArrayList<Operator>)t.parents().clone();
			for (Operator o : parents)
			{
				o.removeChild(op);
			}
			
			for (Operator o : parents)
			{
				NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				ArrayList<Integer> nodeList = t.getNodeList(o);
				for (int node : nodeList)
				{
					TableScanOperator table = t.clone();
					table.setNode(node);
					NetworkSendOperator send = new NetworkSendOperator(node, meta);
					try
					{
						send.add(table);
						receive.add(send);
					}
					catch(Exception e)
					{
						e.printStackTrace();
						System.exit(1);
					}
					table.addActiveDevices(t.getDeviceList(o));
					CNFFilter cnf = t.getCNFForParent(o);
					if (cnf != null)
					{
						table.setCNFForParent(send, cnf);
					}
				}
				
				try
				{
					o.add(receive);
				}
				catch(Exception e)
				{
					e.printStackTrace();
					System.exit(1);
				}
			}
		}
		else
		{
			for (Operator o : (ArrayList<Operator>)op.children().clone())
			{
				updateTree(o);
			}
		}
	}
	
	protected void setActiveDevices(TableScanOperator t, CNFFilter filter, Operator o)
	{
		if (t.isSingleDeviceSet())
		{
			t.addActiveDeviceForParent(t.getSingleDevice(), o);
			return;
		}
		
		if (t.deviceIsHash())
		{
			if (filter != null && filter.hashFiltersPartitions(t.getDeviceHash()))
			{
				if (t.allDevices())
				{
					t.addActiveDeviceForParent((int)(filter.getPartitionHash() % t.getNumDevices()), o);
				}
				else
				{
					t.addActiveDeviceForParent(t.deviceSet().get((int)(filter.getPartitionHash() % t.getNumDevices())), o);
				}
			}
			else
			{
				if (t.allDevices())
				{
					int i = 0;
					while (i < t.getNumDevices())
					{
						t.addActiveDeviceForParent(i, o);
						i++;
					}
				}
				else
				{
					for (int j : t.deviceSet())
					{
						t.addActiveDeviceForParent(j, o);
					}
				}
			}
		}
		else
		{
			//device range
			if (filter != null && filter.rangeFiltersPartitions(t.getDeviceRangeCol()))
			{
				ArrayList<Filter> f = filter.getRangeFilters(); 
				//handle devices all or devices set
				ArrayList<Integer> devices = t.getDevicesMatchingRangeFilters(f);
				t.addActiveDevicesForParent(devices, o);
			}
			else
			{
				if (t.allDevices())
				{
					int i = 0;
					while (i < t.getNumDevices())
					{
						t.addActiveDeviceForParent(i, o);
						i++;
					}
				}
				else
				{
					for (int j : t.deviceSet())
					{
						t.addActiveDeviceForParent(j, o);
					}
				}
			}
		}
	}
	
	protected void setActiveNodes(TableScanOperator t, CNFFilter filter, Operator o, ArrayList<ArrayList<Integer>> nodeLists)
	{
		if (t.isSingleNodeSet())
		{
			for (ArrayList<Integer> nodeList : nodeLists)
			{
				t.addActiveNodeForParent(nodeList.get(t.getSingleNode()), o);
			}
			return;
		}
		
		if (t.nodeIsHash())
		{
			if (filter != null && filter.hashFiltersPartitions(t.getNodeHash()))
			{
				if (t.allNodes())
				{
					for (ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get((int)(filter.getPartitionHash() % nodeList.size())), o);
					}
				}
				else
				{
					for (ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get(t.nodeSet().get((int)(filter.getPartitionHash() % t.getNumNodes()))), o);
					}
				}
			}
			else
			{
				if (t.allNodes())
				{
					for (ArrayList<Integer> nodeList : nodeLists)
					{
						for (Integer node : nodeList)
						{
							t.addActiveNodeForParent(node, o);
						}
					}
				}
				else
				{
					for (int j : t.nodeSet())
					{
						for (ArrayList<Integer> nodeList : nodeLists)
						{
							t.addActiveNodeForParent(nodeList.get(j), o);
						}
					}
				}
			}
		}
		else
		{
			//node range
			if (filter != null && filter.rangeFiltersPartitions(t.getNodeRangeCol()))
			{
				ArrayList<Filter> f = filter.getRangeFilters(); 
				//handle nodes all or nodes set
				ArrayList<Integer> nodes = t.getNodesMatchingRangeFilters(f);
				for (Integer node : nodes)
				{
					for (ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get(node), o);
					}
				}
			}
			else
			{
				if (t.allNodes())
				{
					for (ArrayList<Integer> nodeList : nodeLists)
					{
						for (Integer node : nodeList)
						{
							t.addActiveNodeForParent(node, o);
						}
					}
				}
				else
				{
					for (int j : t.nodeSet())
					{
						for (ArrayList<Integer> nodeList : nodeLists)
						{
							t.addActiveNodeForParent(nodeList.get(j), o);
						}
					}
				}
			}
		}
	}
	
	protected ArrayList<Integer> determineHashMapEntries(TableScanOperator t, CNFFilter filter)
	{
		if (t.isSingleNodeGroupSet())
		{
			ArrayList<Integer> retval = new ArrayList<Integer>(1);
			retval.add(t.getSingleNodeGroup());
			return retval;
		}
		
		if (t.nodeGroupIsHash())
		{
			if (filter != null && filter.hashFiltersPartitions(t.getNodeGroupHash()))
			{
				ArrayList<Integer> retval = new ArrayList<Integer>(1);
				retval.add(t.nodeGroupSet().get((int)(filter.getPartitionHash() % t.getNumNodeGroups())));
				return retval;
			}
			else
			{
				ArrayList<Integer> retval = new ArrayList<Integer>(t.getNodeGroupHashMap().size());
				for (int id : t.getNodeGroupHashMap().keySet())
				{
					retval.add(id);
				}
				
				return retval;
			}
		}
		else
		{
			//node group range
			if (filter != null && filter.rangeFiltersPartitions(t.getNodeGroupRangeCol()))
			{
				ArrayList<Filter> f = filter.getRangeFilters(); 
				ArrayList<Integer> devices = t.getNodeGroupsMatchingRangeFilters(f);
				return devices;
			}
			else
			{
				ArrayList<Integer> retval = new ArrayList<Integer>(t.getNodeGroupHashMap().size());
				for (int id : t.getNodeGroupHashMap().keySet())
				{
					retval.add(id);
				}
				
				return retval;
			}
		}
	}
}

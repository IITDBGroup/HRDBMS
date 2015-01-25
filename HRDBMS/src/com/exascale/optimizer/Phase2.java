package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Transaction;

public final class Phase2
{
	private final RootOperator root;
	private final MetaData meta;
	private Transaction tx;

	public Phase2(RootOperator root, Transaction tx)
	{
		this.root = root;
		this.tx = tx;
		meta = root.getMeta();
	}

	public void optimize() throws Exception
	{
		setPartitionMetaData(root);
		updateTree(root);
		//HRDBMSWorker.logger.debug("Exiting P2: ");
		//Phase1.printTree(root, 0);
	}

	private ArrayList<Integer> determineHashMapEntries(TableScanOperator t, CNFFilter filter) throws Exception
	{
		if (t.isSingleNodeGroupSet())
		{
			final ArrayList<Integer> retval = new ArrayList<Integer>(1);
			retval.add(t.getSingleNodeGroup());
			return retval;
		}

		if (t.nodeGroupIsHash())
		{
			if (filter != null && filter.hashFiltersPartitions(t.getNodeGroupHash()))
			{
				final ArrayList<Integer> retval = new ArrayList<Integer>(1);
				retval.add(t.nodeGroupSet().get((int)(filter.getPartitionHash() % t.getNumNodeGroups())));
				return retval;
			}
			else
			{
				final ArrayList<Integer> retval = new ArrayList<Integer>(t.getNodeGroupHashMap().size());
				for (final int id : t.getNodeGroupHashMap().keySet())
				{
					retval.add(id);
				}

				return retval;
			}
		}
		else
		{
			// node group range
			if (filter != null && filter.rangeFiltersPartitions(t.getNodeGroupRangeCol()))
			{
				final ArrayList<Filter> f = filter.getRangeFilters();
				final ArrayList<Integer> devices = t.getNodeGroupsMatchingRangeFilters(f);
				return devices;
			}
			else
			{
				final ArrayList<Integer> retval = new ArrayList<Integer>(t.getNodeGroupHashMap().size());
				for (final int id : t.getNodeGroupHashMap().keySet())
				{
					retval.add(id);
				}

				return retval;
			}
		}
	}

	private void setActiveDevices(TableScanOperator t, CNFFilter filter, Operator o) throws Exception
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
					for (final int j : t.deviceSet())
					{
						t.addActiveDeviceForParent(j, o);
					}
				}
			}
		}
		else
		{
			// device range
			if (filter != null && filter.rangeFiltersPartitions(t.getDeviceRangeCol()))
			{
				final ArrayList<Filter> f = filter.getRangeFilters();
				// handle devices all or devices set
				final ArrayList<Integer> devices = t.getDevicesMatchingRangeFilters(f);
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
					for (final int j : t.deviceSet())
					{
						t.addActiveDeviceForParent(j, o);
					}
				}
			}
		}
	}

	private void setActiveNodes(TableScanOperator t, CNFFilter filter, Operator o, ArrayList<ArrayList<Integer>> nodeLists) throws Exception
	{
		if (t.isSingleNodeSet())
		{
			for (final ArrayList<Integer> nodeList : nodeLists)
			{
				if (t.getSingleNode() == -1)
				{
					t.addActiveNodeForParent(-1, o);
				}
				else
				{
					t.addActiveNodeForParent(nodeList.get(t.getSingleNode()), o);
				}
			}
			return;
		}

		if (t.nodeIsHash())
		{
			if (filter != null && filter.hashFiltersPartitions(t.getNodeHash()))
			{
				if (t.allNodes())
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get((int)(filter.getPartitionHash() % nodeList.size())), o);
					}
				}
				else
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get(t.nodeSet().get((int)(filter.getPartitionHash() % t.getNumNodes()))), o);
					}
				}
			}
			else
			{
				if (t.allNodes())
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						for (final Integer node : nodeList)
						{
							t.addActiveNodeForParent(node, o);
						}
					}
				}
				else
				{
					for (final int j : t.nodeSet())
					{
						for (final ArrayList<Integer> nodeList : nodeLists)
						{
							t.addActiveNodeForParent(nodeList.get(j), o);
						}
					}
				}
			}
		}
		else
		{
			// node range
			if (filter != null && filter.rangeFiltersPartitions(t.getNodeRangeCol()))
			{
				final ArrayList<Filter> f = filter.getRangeFilters();
				// handle nodes all or nodes set
				final ArrayList<Integer> nodes = t.getNodesMatchingRangeFilters(f);
				for (final Integer node : nodes)
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						t.addActiveNodeForParent(nodeList.get(node), o);
					}
				}
			}
			else
			{
				if (t.allNodes())
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						for (final Integer node : nodeList)
						{
							t.addActiveNodeForParent(node, o);
						}
					}
				}
				else
				{
					for (final int j : t.nodeSet())
					{
						for (final ArrayList<Integer> nodeList : nodeLists)
						{
							t.addActiveNodeForParent(nodeList.get(j), o);
						}
					}
				}
			}
		}
	}

	private void setPartitionMetaData(Operator op) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			final TableScanOperator t = (TableScanOperator)op;
			if (!t.metaDataSet())
			{
				t.setMetaData(tx);
			}
		}
		else
		{
			for (final Operator o : op.children())
			{
				setPartitionMetaData(o);
			}
		}
	}

	private void updateTree(Operator op) throws Exception
	{
		if (op instanceof TableScanOperator)
		{
			final TableScanOperator t = (TableScanOperator)op;
			if (t.phase2Done())
			{
				return;
			}

			t.setPhase2Done();
			for (final Operator o : t.parents())
			{
				final CNFFilter filter = t.getCNFForParent(o);
				if (t.noNodeGroupSet())
				{
					if (t.anyNode())
					{
						int i = Math.abs(ThreadLocalRandom.current().nextInt());
						i = i % MetaData.numWorkerNodes;
						t.addActiveNodeForParent(i, o);
						setActiveDevices(t, filter, o);
					}
					else
					{
						// node is hash or range
						// node is set or ALL
						ArrayList<Integer> nodeList = t.getNodeList();
						if (nodeList.get(0) == MetaData.PartitionMetaData.NODE_ALL)
						{
							nodeList = MetaData.getNodesForTable(t.getSchema(), t.getTable(), tx);
						}
						final ArrayList<ArrayList<Integer>> nodeLists = new ArrayList<ArrayList<Integer>>(1);
						nodeLists.add(nodeList);
						setActiveNodes(t, filter, o, nodeLists);
						setActiveDevices(t, filter, o);
					}
				}
				else
				{
					final HashMap<Integer, ArrayList<Integer>> nodeGroupHashMap = t.getNodeGroupHashMap();
					final ArrayList<Integer> entries = determineHashMapEntries(t, filter);
					final ArrayList<ArrayList<Integer>> nodeLists = new ArrayList<ArrayList<Integer>>(entries.size());
					for (final int entry : entries)
					{
						nodeLists.add(nodeGroupHashMap.get(entry));
					}
					setActiveNodes(t, filter, o, nodeLists);
					setActiveDevices(t, filter, o);
				}
			}

			for (final Operator o : t.parents())
			{
				// get rid of duplicates in node and device lists
				final HashSet hs = new HashSet();
				final ArrayList<Integer> nodeList = t.getNodeList(o);
				hs.addAll(nodeList);
				nodeList.clear();
				nodeList.addAll(hs);
				hs.clear();
				final ArrayList<Integer> deviceList = t.getDeviceList(o);
				hs.addAll(deviceList);
				deviceList.clear();
				deviceList.addAll(hs);
			}

			final ArrayList<Operator> parents = (ArrayList<Operator>)t.parents().clone();
			for (final Operator o : parents)
			{
				o.removeChild(op);
			}

			for (final Operator o : parents)
			{
				final NetworkReceiveOperator receive = new NetworkReceiveOperator(meta);
				final ArrayList<Integer> nodeList = t.getNodeList(o);
				for (final int node : nodeList)
				{
					final TableScanOperator table = t.clone();
					table.setNode(node);
					final NetworkSendOperator send = new NetworkSendOperator(node, meta);
					try
					{
						send.add(table);
						receive.add(send);
					}
					catch (final Exception e)
					{
						HRDBMSWorker.logger.error("", e);
						throw e;
					}
					table.addActiveDevices(t.getDeviceList(o));
					final CNFFilter cnf = t.getCNFForParent(o);
					if (cnf != null)
					{
						table.setCNFForParent(send, cnf);
					}
				}

				try
				{
					o.add(receive);
				}
				catch (final Exception e)
				{
					HRDBMSWorker.logger.error("", e);
					throw e;
				}
			}
		}
		else
		{
			for (final Operator o : (ArrayList<Operator>)op.children().clone())
			{
				updateTree(o);
			}
		}
	}
}

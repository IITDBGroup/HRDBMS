package com.exascale.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.tables.Transaction;

/** Splits table scans across nodes according to partitioning scheme. */
public final class Phase2
{
	private final RootOperator root;
	private final MetaData meta;
	private final Transaction tx;

	public Phase2(final RootOperator root, final Transaction tx)
	{
		this.root = root;
		this.tx = tx;
		meta = root.getMeta();
	}

	private static ArrayList<Integer> determineHashMapEntries(final TableScanOperator t, final CNFFilter filter) throws Exception
	{
		if (t.isSingleNodeGroupSet())
		{
			final ArrayList<Integer> retval = new ArrayList<>(1);
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
				return new ArrayList<>(t.getNodeGroupHashMap().keySet());
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
				return new ArrayList<>(t.getNodeGroupHashMap().keySet());
			}
		}
	}

	private static void setActiveDevices(final TableScanOperator t, final CNFFilter filter, final Operator o) throws Exception
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
				addDevices(t, o);
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
				addDevices(t, o);
			}
		}
	}

	private static void addDevices(final TableScanOperator tableScanOperator, final Operator operator) {
		if (tableScanOperator.allDevices())
		{
			int i = 0;
			while (i < tableScanOperator.getNumDevices())
			{
				tableScanOperator.addActiveDeviceForParent(i, operator);
				i++;
			}
		}
		else
		{
			for (final int j : tableScanOperator.deviceSet())
			{
				tableScanOperator.addActiveDeviceForParent(j, operator);
			}
		}
	}

	private static void setActiveNodes(final TableScanOperator t, final CNFFilter filter, final Operator o, final ArrayList<ArrayList<Integer>> nodeLists) throws Exception
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
				// HRDBMSWorker.logger.debug("Hash DOES filter partitions");
				if (t.allNodes())
				{
					for (final ArrayList<Integer> nodeList : nodeLists)
					{
						final int pos = (int)(filter.getPartitionHash() % nodeList.size());
						t.addActiveNodeForParent(nodeList.get(pos), o);
						// HRDBMSWorker.logger.debug("Nodelist is " + nodeList);
						// HRDBMSWorker.logger.debug("Only need to look at node
						// " + nodeList.get(pos) + " in position " + pos);
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
				addNodes(t, o, nodeLists);
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
				addNodes(t, o, nodeLists);
			}
		}
	}

	private static void addNodes(final TableScanOperator tableScanOperator, Operator operator, final ArrayList<ArrayList<Integer>> nodeLists) {
		if (tableScanOperator.allNodes())
		{
			for (final ArrayList<Integer> nodeList : nodeLists)
			{
				for (final Integer node : nodeList)
				{
					tableScanOperator.addActiveNodeForParent(node, operator);
				}
			}
		}
		else
		{
			for (final int j : tableScanOperator.nodeSet())
			{
				for (final ArrayList<Integer> nodeList : nodeLists)
				{
					tableScanOperator.addActiveNodeForParent(nodeList.get(j), operator);
				}
			}
		}
	}

	public void optimize() throws Exception
	{
		setPartitionMetaData(root);
		updateTree(root);
		// HRDBMSWorker.logger.debug("Exiting P2: ");
		// Phase1.printTree(root, 0);
	}

	private void setPartitionMetaData(final Operator op) throws Exception
	{
		if (op instanceof AbstractTableScanOperator)
		{
			final AbstractTableScanOperator t = (AbstractTableScanOperator)op;
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

	/** This method does the real work */
	private void updateTree(final Operator op) throws Exception
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
						if (nodeList.get(0) == PartitionMetaData.NODE_ALL)
						{
							nodeList = MetaData.getNodesForTable(t.getSchema(), t.getTable(), tx);
						}
						final ArrayList<ArrayList<Integer>> nodeLists = new ArrayList<>(1);
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

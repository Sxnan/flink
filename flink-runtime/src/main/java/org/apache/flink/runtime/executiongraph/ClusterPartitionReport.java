package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;

public class ClusterPartitionReport implements Serializable {

	private HashMap<IntermediateDataSetID, Collection<ClusterPartitionDescriptor>> map;

	public ClusterPartitionReport(HashMap<IntermediateDataSetID, Collection<ClusterPartitionDescriptor>> map) {
		this.map = map;
	}

	public Collection<ClusterPartitionDescriptor> getClusterPartitionDescriptor(IntermediateDataSetID id) {
		return map.getOrDefault(id, null);
	}

	public Collection<ClusterPartitionDescriptor> getClusterPartitionDescriptor() {
		return map.values().iterator().next();
	}


}

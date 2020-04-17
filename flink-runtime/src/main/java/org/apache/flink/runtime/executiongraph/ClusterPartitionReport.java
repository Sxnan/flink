package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.api.common.PersistentIntermediateResultStore;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class ClusterPartitionReport implements Serializable {

	private HashMap<IntermediateDataSetID, Collection<ClusterPartitionDescriptorImpl>> map;

	public ClusterPartitionReport(HashMap<IntermediateDataSetID, Collection<ClusterPartitionDescriptorImpl>> map) {
		this.map = map;
	}

	public Collection<ClusterPartitionDescriptorImpl> getClusterPartitionDescriptor(IntermediateDataSetID id) {
		return map.getOrDefault(id, null);
	}

	public Collection<ClusterPartitionDescriptorImpl> getClusterPartitionDescriptor() {
		return map.values().iterator().next();
	}

	public PersistentIntermediateResultStore toPersistentIntermediateResultStore() throws IOException {
		Map<AbstractID, Collection<SerializedValue<ClusterPartitionDescriptor>>> res = new HashMap<>();
		for (Map.Entry<IntermediateDataSetID, Collection<ClusterPartitionDescriptorImpl>> entry : map.entrySet()) {
			Collection<ClusterPartitionDescriptorImpl> clusterPartitionDescriptors = entry.getValue();
			Collection<SerializedValue<ClusterPartitionDescriptor>> value = new HashSet<>();
			for (ClusterPartitionDescriptor clusterPartitionDescriptor : clusterPartitionDescriptors) {
				value.add(new SerializedValue<>(clusterPartitionDescriptor));
			}
			res.put(entry.getKey(), value);
		}

		return new PersistentIntermediateResultStore(res);
	}


}

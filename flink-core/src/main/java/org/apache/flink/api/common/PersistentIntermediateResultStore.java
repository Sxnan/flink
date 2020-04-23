package org.apache.flink.api.common;

import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class PersistentIntermediateResultStore {
	final private Map<AbstractID, Collection<SerializedValue<ClusterPartitionDescriptor>>> map;

	public PersistentIntermediateResultStore() {
		map = new HashMap<>();
	}

	public PersistentIntermediateResultStore(Map<AbstractID,
		Collection<SerializedValue<ClusterPartitionDescriptor>>> map) {
		this.map = map;
	}

	public Map<AbstractID, Collection<SerializedValue<ClusterPartitionDescriptor>>> getMap() {
		return map;
	}
}

package org.apache.flink.table.api;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class CacheManager {

	Map<String, Table> tableToCache;
	Map<Table, Collection<SerializedValue<ClusterPartitionDescriptor>>> cachedTable;

	public CacheManager() {
		tableToCache = new HashMap<>();
		cachedTable = new HashMap<>();
	}

	public void addTableToCache(AbstractID id, Table table) {
		tableToCache.putIfAbsent(id.toHexString(), table);
	}

	public void tableCached(AbstractID id, Collection<SerializedValue<ClusterPartitionDescriptor>> descriptor) {
		final Table table = tableToCache.get(id.toHexString());
		Preconditions.checkNotNull(table, "the table is not register to be cahced");
		tableToCache.remove(id.toHexString());
		final Collection<SerializedValue<ClusterPartitionDescriptor>> prev =
			cachedTable.putIfAbsent(table, descriptor);
		Preconditions.checkState(prev == null,"the table cannot be cached twice");
	}

	public Collection<SerializedValue<ClusterPartitionDescriptor>> getClusterPartitionDescriptor(Table table) {
		return cachedTable.get(table);
	}
}

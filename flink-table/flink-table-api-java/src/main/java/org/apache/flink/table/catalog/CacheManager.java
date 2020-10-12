package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.table.operations.CacheQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Internal
public class CacheManager {

	Set<CacheQueryOperation> tableToCache = new HashSet<>();
	Map<CacheQueryOperation, Collection<ClusterPartitionDescriptor>> cachedTable = new HashMap<>();

	public void addTableToCache(QueryOperation queryOperation) {
		Preconditions.checkState(queryOperation instanceof CacheQueryOperation);
		tableToCache.add((CacheQueryOperation) queryOperation);
	}

	public boolean isCached(CacheQueryOperation cacheQueryOperation) {
		return cachedTable.containsKey(cacheQueryOperation);
	}
}

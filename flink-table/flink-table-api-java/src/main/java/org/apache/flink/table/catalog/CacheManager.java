package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.table.operations.CacheQueryOperation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

@Internal
public class CacheManager {

	Map<AbstractID, CacheQueryOperation> tableToCache = new HashMap<>();
	Map<CacheQueryOperation, PersistedIntermediateResultDescriptor> cachedTable = new HashMap<>();

	public void addTableToCache(QueryOperation queryOperation) {
		Preconditions.checkState(queryOperation instanceof CacheQueryOperation);

		final CacheQueryOperation cacheQueryOperation = (CacheQueryOperation) queryOperation;
		tableToCache.put(cacheQueryOperation.getIntermediateDataSetId(), cacheQueryOperation);
	}

	public void markTableCached(AbstractID intermediateDataSetID,
								PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor) {
		final CacheQueryOperation cachedQueryOperation = tableToCache.remove(new AbstractID(intermediateDataSetID));
		cachedTable.put(cachedQueryOperation, persistedIntermediateResultDescriptor);
	}

	public PersistedIntermediateResultDescriptor getPersistedIntermediateResultDescriptor(
		CacheQueryOperation queryOperation) {
		return cachedTable.get(queryOperation);
	}


	public boolean isCached(CacheQueryOperation cacheQueryOperation) {
		return cachedTable.containsKey(cacheQueryOperation);
	}
}

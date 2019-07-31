package org.apache.flink.table.api;

import java.util.Collection;
import java.util.Map;

/**
 * The interface is used by the CacheManager whenever some table cache are invalidated.
 */
public interface TableCleanUpHook {

	/**
	 * Delete the physical storage for tables with the given table names.
	 */
	void clean(Collection<String> tableNames, Map<String, Map<String, String>> properties);
}

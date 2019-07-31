package org.apache.flink.table.api;

import java.util.Map;

/**
 * The createTable is invoked by the CacheManager before creating the TableSink and TableSource.
 */
public interface TableCreationHook {

	/**
	 * Create the physical location to store the table and return the
	 * configuration that encodes the given table name.
	 */
	Map<String, String> createTable(String tableName, Map<String, String> properties);
}

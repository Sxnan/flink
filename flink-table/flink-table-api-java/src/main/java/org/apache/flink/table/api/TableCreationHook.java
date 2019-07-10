package org.apache.flink.table.api;

import java.util.Map;

public interface TableCreationHook {
	Map<String, String> createTable(String tableName, Map<String, String> properties);
}

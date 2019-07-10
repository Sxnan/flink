package org.apache.flink.table.api;

import java.util.Collection;
import java.util.Map;

public interface CleanUpHook {
	void clean(Collection<String> tablesToDelete, Map<String, Map<String, String>> properties);
}

package org.apache.flink.table.api;

import java.util.Map;

public interface Configurable {
	void configure(Map<String, ?> configs);
}

package org.apache.flink.table.api;

import java.util.Map;

/**
 * The Configurable interface indicates that the class can be instantiated by reflection.
 * And it needs to take some configuration parameters to configure itself.
 *
 */
public interface Configurable {

	/**
	 * Configure the class with the given key-value pairs
	 */
	void configure(Map<String, String> configs);
}

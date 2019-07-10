package org.apache.flink.table.api;

import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;

public interface IntermediateResultStorage {
	TableSourceFactory getTableSourceFactory();
	TableSinkFactory getTableSinkFactory();
	CleanUpHook getCleanUpHook();
	TableCreationHook getTableCreationHook();
}

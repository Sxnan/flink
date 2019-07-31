package org.apache.flink.table.api;

import org.apache.flink.table.factories.TableFactory;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;

/**
 * This interface represent an backend end storage where the intermediate result stored upon caching.
 * It provides the necessary methods for the CacheManager to substitute cached table with TableSink and TableSource.
 */
public interface IntermediateResultStorage extends Configurable, TableFactory {

	/**
	 * Get the TableSourceFactory which the cache manager uses to create the TableSource to replace the
	 * cached table.
	 */
	TableSourceFactory getTableSourceFactory();

	/**
	 * Get the TableSinkFactory which the cache manager uses to create the TableSink when a table need to be
	 * cached.
	 */
	TableSinkFactory getTableSinkFactory();

	/**
	 * The cache manager gets the TableCleanUpHook and invoke the cleanup method to reclaim the space when
	 * some cached tables are invalidate.
	 */
	TableCleanUpHook getCleanUpHook();

	/**
	 * Table cache manager gets the TableCreationHook and invoke the createTable method before creating the
	 * TableSource.
	 */
	TableCreationHook getTableCreationHook();
}

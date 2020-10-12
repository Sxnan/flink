package org.apache.flink.table.api;

public interface CachedTable extends Table {

	/**
	 * Manually invalidate the cache of this table to release the physical resources. Users are
	 * not required to invoke this method to release physical resource unless they want to. The
	 * table caches are cleared when user program exits.
	 *
	 * Note: After invalidated, the cache may be re-created if this table is used again.
	 */
	void invalidateCache();
}

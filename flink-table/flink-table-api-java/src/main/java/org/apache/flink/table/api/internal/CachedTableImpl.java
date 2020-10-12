package org.apache.flink.table.api.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.CachedTable;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.resolver.LookupCallResolver;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.utils.OperationTreeBuilder;
import org.apache.flink.util.Preconditions;

@Internal
public class CachedTableImpl extends TableImpl implements CachedTable {
	private CachedTableImpl(
		TableEnvironmentInternal tableEnvironment,
		QueryOperation operationTree,
		OperationTreeBuilder operationTreeBuilder,
		LookupCallResolver lookupResolver) {
		super(tableEnvironment, operationTree, operationTreeBuilder, lookupResolver);
	}

	public static CachedTableImpl createCachedTable(TableImpl table) {
		final TableEnvironment tableEnvironment = table.getTableEnvironment();
		Preconditions.checkState(tableEnvironment instanceof TableEnvironmentInternal);

		return new CachedTableImpl(
			(TableEnvironmentInternal) tableEnvironment,
			table.getQueryOperation(),
			table.getOperationTreeBuilder(),
			table.getLookupResolver()
		);
	}

	@Override
	public void invalidateCache() {
		// TODO: implement invalidate cache
	}
}

package org.apache.flink.table.planner.sources;

import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.streaming.api.functions.source.CacheSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;

public class CacheTableSource implements ScanTableSource {

	private TableSchema tableSchema;
	private PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor;

	public CacheTableSource(TableSchema tableSchema,
							PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor) {

		this.tableSchema = tableSchema;
		this.persistedIntermediateResultDescriptor = persistedIntermediateResultDescriptor;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return SourceFunctionProvider.of(new CacheSourceFunction<>(persistedIntermediateResultDescriptor),
			true);
	}

	@Override
	public DynamicTableSource copy() {
		return new CacheTableSource(tableSchema, persistedIntermediateResultDescriptor);
	}

	@Override
	public String asSummaryString() {
		return "CacheTableSource";
	}
}

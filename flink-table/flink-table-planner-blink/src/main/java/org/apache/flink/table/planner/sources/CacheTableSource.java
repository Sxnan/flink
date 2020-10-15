package org.apache.flink.table.planner.sources;

import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.CacheSourceFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

public class CacheTableSource implements StreamTableSource<Row> {

	private TableSchema tableSchema;
	private PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor;

	public CacheTableSource(TableSchema tableSchema,
							PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor) {

		this.tableSchema = tableSchema;
		this.persistedIntermediateResultDescriptor = persistedIntermediateResultDescriptor;
	}

	@Override
	public DataType getProducedDataType() {
		return tableSchema.toRowDataType();
	}

	@Override
	public boolean isBounded() {
		return true;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
		return execEnv.addSource(new CacheSourceFunction<>(persistedIntermediateResultDescriptor),
			tableSchema.toRowType());
	}

}

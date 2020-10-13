package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.CacheSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.util.AbstractID;

public class CacheTableSink<T> implements StreamTableSink<T> {

	private AbstractID intermediateDataSetId;
	private TableSchema tableSchema;

	public CacheTableSink(AbstractID intermediateDataSetId, TableSchema tableSchema) {
		this.intermediateDataSetId = intermediateDataSetId;
		this.tableSchema = tableSchema;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

	@Override
	public TypeInformation<T> getOutputType() {
		return InternalTypeInfo.of(tableSchema.toRowDataType().getLogicalType());
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<T> dataStream) {
		return dataStream.addSink(new CacheSinkFunction<>(intermediateDataSetId));
	}

	@Override
	public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		// do nothing
		return this;
	}
}

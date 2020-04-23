package org.apache.flink.table.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.AbstractID;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

public class CacheTableSinkFactory<T> implements TableSinkFactory<T> {
	private static final String CONNECTOR_VALUE = "cache";

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR, CONNECTOR_VALUE);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		return Collections.singletonList("*");
	}

	@Override
	public TableSink<T> createTableSink(Context context) {
		final Long upper = Long.valueOf(context.getTable().getProperties().get("upper"));
		final Long lower = Long.valueOf(context.getTable().getProperties().get("lower"));

		return new CacheTableSink(new AbstractID(lower, upper), context.getTable().getSchema());
	}

	class CacheTableSink implements StreamTableSink<T> {

		private AbstractID id;
		private TableSchema tableSchema;

		CacheTableSink(AbstractID id, TableSchema tableSchema) {
			this.id = id;
			this.tableSchema = tableSchema;
		}

		@Override
		public DataStreamSink<?> consumeDataStream(DataStream<T> dataStream) {
			return dataStream.writeUsingOutputFormat(BlockingShuffleOutputFormat.createOutputFormat(id));
		}

		@Override
		public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
			return this;
		}

		@Override
		public DataType getConsumedDataType() {
			return tableSchema.toRowDataType();
		}

		@Override
		public TableSchema getTableSchema() {
			return tableSchema;
		}
	}
}

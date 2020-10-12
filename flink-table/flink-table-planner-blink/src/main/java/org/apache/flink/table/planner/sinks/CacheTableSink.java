package org.apache.flink.table.planner.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.CacheSinkFunction;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;

public class CacheTableSink<T> implements StreamTableSink<T> {
	@Override
	public DataStreamSink<?> consumeDataStream(DataStream<T> dataStream) {
		return null;
	}

	@Override
	public TableSink<T> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		// do nothing
		return this;
	}
}

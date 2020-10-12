package org.apache.flink.table.planner.sources;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;

public class CacheTableSource<T> implements StreamTableSource<T> {
	@Override
	public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv) {
		return null;
	}

	@Override
	public TableSchema getTableSchema() {
		return null;
	}
}

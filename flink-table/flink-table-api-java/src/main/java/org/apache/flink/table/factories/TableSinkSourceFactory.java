package org.apache.flink.table.factories;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.util.Map;

public abstract class TableSinkSourceFactory implements TableSourceFactory<Row>, TableSinkFactory<Row> {
	@Override
	final public TableSource<Row> createTableSource(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);
		return createTableSource(params.getString("__table__name__"), params.getTableSchema("__schema__"));
	}

	@Override
	final public TableSink<Row> createTableSink(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);
		return createTableSink(params.getString("__table__name__"), params.getTableSchema("__schema__"));
	}

	abstract public TableSource<Row> createTableSource(String tableName, TableSchema tableSchema);

	abstract public TableSink<Row> createTableSink(String tableName, TableSchema tableSchema);
}

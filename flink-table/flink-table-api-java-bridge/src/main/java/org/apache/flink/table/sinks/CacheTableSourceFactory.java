package org.apache.flink.table.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.BlockingShuffleInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR;

public class CacheTableSourceFactory<T> implements TableSourceFactory<T> {

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
	public TableSource createTableSource(Context context) {
		final CatalogTable table = context.getTable();

		return new CacheTableSource(table.getSchema());
	}

	class CacheTableSource implements StreamTableSource<Row> {

		private TableSchema tableSchema;

		CacheTableSource(TableSchema tableSchema) {
			this.tableSchema = tableSchema;
		}

		@Override
		public TableSchema getTableSchema() {
			return tableSchema;
		}

		@Override
		public boolean isBounded() {
			return true;
		}

		@Override
		public DataType getProducedDataType() {
			return tableSchema.toRowDataType();
		}

		@Override
		public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
			TypeInformation<Row> typeInformation =
				(TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(getProducedDataType());
			return execEnv.createInput(new BlockingShuffleInputFormat<>(), typeInformation);
		}
	}
}

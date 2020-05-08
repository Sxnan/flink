package org.apache.flink.table.sinks;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.BlockingShuffleInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CacheManager;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
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
		final Long upper = Long.valueOf(context.getTable().getProperties().get("upper"));
		final Long lower = Long.valueOf(context.getTable().getProperties().get("lower"));


		return new CacheTableSource(table.getSchema(), new AbstractID(lower, upper));
	}

	class CacheTableSource implements StreamTableSource<Row> {

		private TableSchema tableSchema;
		private AbstractID id;

		CacheTableSource(TableSchema tableSchema,
						 AbstractID id) {
			this.tableSchema = tableSchema;
			this.id = id;
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

			final Collection<SerializedValue<ClusterPartitionDescriptor>> clusterPartitionDescriptor
				= CacheManager.getInstance().getClusterPartitionDescriptor(id);

			return execEnv.createInput(new BlockingShuffleInputFormat<>(clusterPartitionDescriptor), typeInformation);
		}
	}
}

package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.CleanUpHook;
import org.apache.flink.table.api.IntermediateResultStorage;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableCreationHook;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.CsvBatchTableSinkFactory;
import org.apache.flink.table.sources.CsvBatchTableSourceFactory;

public class SimpleCacheExample {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		tEnv.registerTableSinkSourceFactory(new MyIntermediateResultStorage());

		DataSet<WC> input = env.fromElements(
			new WC("a", 1),
			new WC("a", 1),
			new WC("a", 1),
			new WC("b", 1),
			new WC("b", 1),
			new WC("c", 1));

		Table sourceTable = tEnv.fromDataSet(input);
		sourceTable.cache();

		Table countedTable = sourceTable.groupBy("word")
			.select("word, frequency.sum as frequency")
			.cache();

		DataSet<WC> result = tEnv.toDataSet(countedTable.filter("frequency > 1"), WC.class);
		result.print();

		// TODO: this should be called by Execution Environment / Table Environment when the job finished
		tEnv.getCacheManager().markAllTableCached();

		System.out.println("-------------------");
		result = tEnv.toDataSet(countedTable.filter("frequency <= 1"), WC.class);
		result.print();
	}

	/**
	 * Simple POJO containing a word and its respective count.
	 */
	public static class WC {
		public String word;
		public long frequency;

		// public constructor to make it a Flink POJO
		public WC() {}

		public WC(String word, long frequency) {
			this.word = word;
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return word + " " + frequency;
		}
	}
}

class MyIntermediateResultStorage implements IntermediateResultStorage {


	private static final String CACHE_FOLDER = "/Users/xuannansu/cache/";
//	private static final String CACHE_FOLDER = "hdfs:///cache";

	@Override
	public TableSourceFactory getTableSourceFactory() {
		return new CsvBatchTableSourceFactory();
	}

	@Override
	public TableSinkFactory getTableSinkFactory() {
		return new CsvBatchTableSinkFactory();
	}

	@Override
	public CleanUpHook getCleanUpHook() {
		return (tablesToDelete, properties) -> {
		};
	}

	@Override
	public TableCreationHook getTableCreationHook() {
		return (tableName, properties) -> {
			DescriptorProperties descriptorProperties = new DescriptorProperties();
			descriptorProperties.putProperties(properties);
			TableSchema tableSchema = descriptorProperties.getTableSchema(Schema.SCHEMA);

			ConnectorDescriptor connectorDescriptor = new FileSystem().path(CACHE_FOLDER + tableName);
			BatchTableDescriptor batchTableDescriptor = new BatchTableDescriptor(null, connectorDescriptor);
			OldCsv formatDescriptor = getFormatDescriptor(tableSchema);
			batchTableDescriptor.withFormat(formatDescriptor);
			descriptorProperties.putProperties(batchTableDescriptor.toProperties());
			return descriptorProperties.asMap();
		};
	}

	private OldCsv getFormatDescriptor(TableSchema tableSchema) {
		String[] fieldNames = tableSchema.getFieldNames();
		TypeInformation[] typeInformations = tableSchema.getFieldTypes();
		OldCsv oldCsv = new OldCsv();

		for (int i = 0; i < fieldNames.length; ++i) {
			oldCsv.field(fieldNames[i], typeInformations[i]);
		}
		return oldCsv;
	}
}

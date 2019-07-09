package org.apache.flink.table.examples.java;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableSinkFactory;
import org.apache.flink.table.factories.TableSourceFactory;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.types.Row;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleCacheExample {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		tEnv.registerTableSinkSourceFactory(new MySourceFactory(), new MySinkFactory());

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

class MySourceFactory implements TableSourceFactory<Row> {
	final private String tempDir = "/Users/xuannansu/cache";

	@Override
	public TableSource<Row> createTableSource(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);
		TableSchema tableSchema = params.getTableSchema("__schema__");
		String tableName = params.getString("__table__name__");
		String tablePath = Paths.get(tempDir, tableName).toString();

		return new CsvTableSource(tablePath, tableSchema.getFieldNames(), tableSchema.getFieldTypes());
	}

	@Override
	public Map<String, String> requiredContext() {
		return new HashMap<>();
	}

	@Override
	public List<String> supportedProperties() {
		return null;
	}
}

class MySinkFactory implements TableSinkFactory<Row> {
	final private String tempDir = "/Users/xuannansu/cache";

	@Override
	public TableSink<Row> createTableSink(Map<String, String> properties) {
		DescriptorProperties params = new DescriptorProperties();
		params.putProperties(properties);
		TableSchema tableSchema = params.getTableSchema("__schema__");
		String tableName = params.getString("__table__name__");

		String tablePath = Paths.get(tempDir, tableName).toString();
		String[] fieldNames = tableSchema.getFieldNames();
		TypeInformation[] typeInformations = tableSchema.getFieldTypes();
		return new CsvTableSink(tablePath, ",").configure(fieldNames, typeInformations);
	}

	@Override
	public Map<String, String> requiredContext() {
		return null;
	}

	@Override
	public List<String> supportedProperties() {
		return null;
	}
}

package org.apache.flink.table.examples.java;

import com.google.common.io.Files;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.RowCsvInputFormat;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.io.TextOutputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.factories.TableSinkSourceFactory;
import org.apache.flink.table.sinks.*;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SimpleCacheExample {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

		tEnv.registerTableSinkSourceFactory(new MySinkSourceFactory());

		DataSet<WC> input = env.fromElements(
			new WC("a", 1),
			new WC("a", 1),
			new WC("a", 1),
			new WC("b", 1),
			new WC("b", 1),
			new WC("c", 1));

		Table sourceTable = tEnv.fromDataSet(input);
		Table countedTable = sourceTable.groupBy("word")
			.select("word, frequency.sum as frequency")
			.cache();

		DataSet<WC> result = tEnv.toDataSet(countedTable.filter("frequency > 1"), WC.class);
		result.print();

		// TODO: this should be called by Execution Environnment / Table Environment when the job finished
		tEnv.getCacheManager().finishAllCaching();

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

class MySinkSourceFactory extends TableSinkSourceFactory {

	final private String tempDir = "/Users/xuannansu/cache";

	@Override
	public TableSource<Row> createTableSource(String tableName, TableSchema tableSchema) {
		String tablePath = Paths.get(tempDir, tableName).toString();
		return new CsvTableSource(tablePath, tableSchema.getFieldNames(), tableSchema.getFieldTypes());
	}

	@Override
	public TableSink<Row> createTableSink(String tableName, TableSchema tableSchema) {
		String tablePath = Paths.get(tempDir, tableName).toString();
		return new MyTableSink(tablePath, tableSchema);
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

class MyTableSink extends OutputFormatTableSink<Row> {

	private String path;
	private TableSchema tableSchame;

	MyTableSink(String path, TableSchema tableSchema) {
		this.path = path;
		this.tableSchame = tableSchema;
	}

	@Override
	public OutputFormat<Row> getOutputFormat() {
		return new TextOutputFormat<>(new Path(path));
	}

	@Override
	public TableSink<Row> configure(String[] fieldNames, TypeInformation<?>[] fieldTypes) {
		return null;
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchame;
	}

	@Override
	public DataType getConsumedDataType() {
		return tableSchame.toRowDataType();
	}
}

class MyTableSource extends InputFormatTableSource<Row> {

	private String path;
	private TableSchema tableSchema;

	MyTableSource(String path, TableSchema tableSchema) {
		this.path = path;
		this.tableSchema = tableSchema;
	}

	@Override
	public InputFormat<Row, ?> getInputFormat() {
		return new RowCsvInputFormat(new Path(path), tableSchema.getFieldTypes());
	}

	@Override
	public TableSchema getTableSchema() {
		return tableSchema;
	}

}

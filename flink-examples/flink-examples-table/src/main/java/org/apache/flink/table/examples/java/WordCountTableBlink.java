package org.apache.flink.table.examples.java;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

public class WordCountTableBlink {
	public static void main(String[] args) throws Exception {

		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inBatchMode()
			.useBlinkPlanner()
			.build();
		TableEnvironment tEnv = TableEnvironment.create(settings);

		final Schema schema = new Schema()
			.field("word", DataTypes.STRING())
			.field("count", DataTypes.INT());

		tEnv.connect(new FileSystem().path("/tmp/input"))
			.withFormat(new OldCsv())
			.withSchema(schema)
			.createTemporaryTable("input");

		tEnv.connect(new FileSystem().path("/tmp/output"))
			.withFormat(new OldCsv())
			.withSchema(schema)
			.createTemporaryTable("output");

		tEnv.connect(new FileSystem().path("/tmp/output2"))
			.withFormat(new OldCsv())
			.withSchema(schema)
			.createTemporaryTable("output2");

		Table t = tEnv.from("input")
			.groupBy("word")
			.select("word, count.sum as count");

		Table cachedTable = t.cache();

		t.filter("count = 2")
			.insertInto("output");

		tEnv.execute("first job");

		cachedTable.filter("count > 1")
			.insertInto("output2");

		tEnv.execute("second job");

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
			return "WC " + word + " " + frequency;
		}
	}
}

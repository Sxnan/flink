package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.AbstractID;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CacheIntermediateResultProcessorTest {

    @Test
    public void process() {

		final CacheIntermediateResultProcessor cacheIntermediateResultProcessor =
			new CacheIntermediateResultProcessor();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

		final DataStream<?> source = env.addSource(new NoOpSource());

		source.addSink(new DiscardingSink<>());
		source.addSink(new NoOpSinkFunction(BlockingShuffleOutputFormat.createOutputFormat(new AbstractID())));

		final StreamGraph streamGraph = env.getStreamGraph();
//		streamGraph.getSourceIDs();

		final StreamNode sourceNode = streamGraph.getStreamNode(streamGraph.getSourceIDs().iterator().next());

		Assert.assertEquals(2, sourceNode.getOutEdges().size());
		sourceNode.getOutEdges().forEach(outEdge -> assertFalse(outEdge.isPersistent()));

		cacheIntermediateResultProcessor.process(streamGraph);
		Assert.assertEquals(1, sourceNode.getOutEdges().size());
		sourceNode.getOutEdges().forEach(outEdge -> assertTrue(outEdge.isPersistent()));

	}

	class NoOpSinkFunction extends OutputFormatSinkFunction {

		public NoOpSinkFunction(OutputFormat format) {
			super(format);
		}
	}

    class NoOpSource implements SourceFunction<Object> {

		@Override
		public void run(SourceContext<Object> ctx) throws Exception {

		}

		@Override
		public void cancel() {

		}
	}
}

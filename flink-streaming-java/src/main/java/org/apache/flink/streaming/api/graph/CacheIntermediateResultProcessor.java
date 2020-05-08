package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.io.BlockingShuffleInputFormat;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleInputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

public class CacheIntermediateResultProcessor {

	void process(StreamGraph streamGraph) {
		Set<Integer> visited = new HashSet<>();
		Queue<StreamNode> remaining = new ArrayDeque<>();

		for (Integer sourceId : streamGraph.getSourceIDs()) {
			remaining.add(streamGraph.getStreamNode(sourceId));
			visited.add(sourceId);
		}

		StreamNode currentNode = remaining.poll();
		while (currentNode != null) {
			StreamNode blockingSink = getBlockingSink(streamGraph, currentNode);
			if (blockingSink != null) {
				streamGraph.removeVertex(blockingSink);
				final List<StreamEdge> inEdges = currentNode.getInEdges();
				Preconditions.checkState(inEdges.size() == 1, "Should only have one input edge");
				StreamNode upstreamNode = streamGraph.getSourceVertex(inEdges.iterator().next());
				streamGraph.removeVertex(currentNode);
				streamGraph.getSinkIDs().remove(currentNode.getId());
				StreamEdge persistentEdge = upstreamNode.getOutEdges().iterator().next();
				persistentEdge.setPersistent(true);

				final BlockingShuffleOutputFormat<?> outputFormat =
					(BlockingShuffleOutputFormat)((OutputFormatOperatorFactory<?>) blockingSink.getOperatorFactory()).getOutputFormat();

				persistentEdge.setIntermediateResultID(outputFormat.getIntermediateDataSetId());
			}

			if (isBlockingSource(currentNode)) {
				processBlockingSource(streamGraph, currentNode);
			}


			for (StreamEdge outEdges : currentNode.getOutEdges()) {
				final StreamNode targetVertex = streamGraph.getTargetVertex(outEdges);
				Integer id = targetVertex.getId();
				if (!visited.contains(id)) {
					remaining.add(streamGraph.getStreamNode(id));
					visited.add(id);
				}
			}
			currentNode = remaining.poll();
		}
	}

	private void processBlockingSource(StreamGraph streamGraph, StreamNode blockingSourceNode) {
		Preconditions.checkState(blockingSourceNode.getOutEdges().size() == 1,
			"There should be only one output edge for the blocking source node");
		StreamNode sourceConversionNode =
			streamGraph.getTargetVertex(blockingSourceNode.getOutEdges().iterator().next());

		Preconditions.checkState(sourceConversionNode.getOutEdges().size() == 1,
			"There should be only one output edge");
		StreamNode cacheConsumerNode = streamGraph
			.getTargetVertex(sourceConversionNode.getOutEdges().iterator().next());

		final TypeSerializer<?> typeSerializerIn = cacheConsumerNode.getTypeSerializerIn(0);
		streamGraph.removeVertex(blockingSourceNode);
		streamGraph.removeVertex(sourceConversionNode);

		streamGraph.addOperator(sourceConversionNode.getId(),
			sourceConversionNode.getSlotSharingGroup(),
			sourceConversionNode.getCoLocationGroup(),
			SimpleOperatorFactory.of(new StreamMap<>((MapFunction<Object, Object>) value -> value)),
			typeSerializerIn,
			typeSerializerIn,
			"");

		streamGraph.setParallelism(sourceConversionNode.getId(), sourceConversionNode.getParallelism());
		streamGraph.setMaxParallelism(sourceConversionNode.getId(), sourceConversionNode.getMaxParallelism());
		streamGraph.addEdge(sourceConversionNode.getId(), cacheConsumerNode.getId(), 1);

		streamGraph.getSourceIDs().remove(blockingSourceNode.getId());
		streamGraph.getSourceIDs().add(sourceConversionNode.getId());


		final InputFormat inputFormat = ((SimpleInputFormatOperatorFactory) blockingSourceNode.getOperatorFactory()).getInputFormat();
		BlockingShuffleInputFormat blockingShuffleInputFormat = (BlockingShuffleInputFormat)inputFormat;

		final StreamNode streamNode = streamGraph.getStreamNode(sourceConversionNode.getId());
		streamNode.setClusterPartitionDescriptor(blockingShuffleInputFormat.getClusterPartitionDescriptor());
	}

	private boolean isBlockingSource(StreamNode currentNode) {
		final StreamOperatorFactory<?> operatorFactory = currentNode.getOperatorFactory();
		if (!(operatorFactory instanceof SimpleInputFormatOperatorFactory)) {
			return false;
		}

		final SimpleInputFormatOperatorFactory<?> simpleInputFormat =
			(SimpleInputFormatOperatorFactory<?>) operatorFactory;

		return simpleInputFormat.getInputFormat() instanceof BlockingShuffleInputFormat;
	}

	private StreamNode getBlockingSink(StreamGraph streamGraph, StreamNode currentNode) {
		for (StreamEdge outEdge : currentNode.getOutEdges()) {
			final StreamNode targetVertex = streamGraph.getTargetVertex(outEdge);
			if(isBlockingSink(targetVertex)) {
				return targetVertex;
			}
		}
		return null;
	}


	private boolean isBlockingSink(StreamNode targetVertex) {

		final StreamOperatorFactory<?> streamOperatorFactory = targetVertex.getOperatorFactory();
		if (!(streamOperatorFactory instanceof OutputFormatOperatorFactory)) {
			return false;
		}
		final OutputFormatOperatorFactory<?> outputFormatOperatorFactory =
			(OutputFormatOperatorFactory<?>) streamOperatorFactory;

		return outputFormatOperatorFactory.getOutputFormat() instanceof BlockingShuffleOutputFormat;
	}
}

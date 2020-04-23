package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
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
				StreamEdge persistentEdge = upstreamNode.getOutEdges().iterator().next();
				persistentEdge.setPersistent(true);

				final BlockingShuffleOutputFormat<?> outputFormat =
					(BlockingShuffleOutputFormat)((OutputFormatOperatorFactory<?>) blockingSink.getOperatorFactory()).getOutputFormat();

				persistentEdge.setIntermediateResultID(outputFormat.getIntermediateDataSetId());
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

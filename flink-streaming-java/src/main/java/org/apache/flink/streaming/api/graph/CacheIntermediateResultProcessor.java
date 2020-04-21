package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.java.io.BlockingShuffleOutputFormat;
import org.apache.flink.streaming.api.operators.OutputFormatOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;

import java.util.ArrayDeque;
import java.util.HashSet;
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
			}

			for (StreamEdge outEdges : currentNode.getOutEdges()) {
				outEdges.setPersistent(true);
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

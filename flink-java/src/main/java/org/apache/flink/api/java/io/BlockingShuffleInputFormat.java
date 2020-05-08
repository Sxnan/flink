package org.apache.flink.api.java.io;

import org.apache.flink.api.common.ClusterPartitionDescriptor;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.Collection;

public class BlockingShuffleInputFormat<OT> implements InputFormat<OT, BlockingShuffleInputSplit> {


	private Collection<SerializedValue<ClusterPartitionDescriptor>> clusterPartitionDescriptor;

	public BlockingShuffleInputFormat(Collection<SerializedValue<ClusterPartitionDescriptor>> clusterPartitionDescriptor) {

		this.clusterPartitionDescriptor = clusterPartitionDescriptor;
	}

	@Override
	public void configure(Configuration parameters) {
		throw new UnsupportedOperationException();
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public BlockingShuffleInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(BlockingShuffleInputSplit[] inputSplits) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void open(BlockingShuffleInputSplit split) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean reachedEnd() throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public OT nextRecord(Object reuse) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException();
	}

	public Collection<SerializedValue<ClusterPartitionDescriptor>> getClusterPartitionDescriptor() {
		return clusterPartitionDescriptor;
	}
}

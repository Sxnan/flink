package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractID;

@Internal
public class CacheSinkFunction<T> implements SinkFunction<T> {
	private final AbstractID intermediateDataSetId;

	public CacheSinkFunction(AbstractID intermediateDataSetId) {
		this.intermediateDataSetId = intermediateDataSetId;
	}

	public AbstractID getIntermediateDataSetId() {
		return intermediateDataSetId;
	}

	@Override
	public void invoke(T value, Context context) throws Exception {
		throw new UnsupportedOperationException("This method should never be called");
	}
}

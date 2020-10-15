package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.PersistedIntermediateResultDescriptor;

@Internal
public class CacheSourceFunction<T> implements SourceFunction<T> {
	private final PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor;

	public CacheSourceFunction(PersistedIntermediateResultDescriptor persistedIntermediateResultDescriptor) {
		this.persistedIntermediateResultDescriptor = persistedIntermediateResultDescriptor;
	}

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		throw new UnsupportedOperationException("This method should never be called");
	}

	@Override
	public void cancel() {
		throw new UnsupportedOperationException("This method should never be called");
	}

	public PersistedIntermediateResultDescriptor getPersistedIntermediateResultDescriptor() {
		return persistedIntermediateResultDescriptor;
	}
}

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.AbstractID;

@Internal
public class CacheSourceFunction<T> implements SourceFunction<T> {
	// TODO: add cluster partition descriptor

	@Override
	public void run(SourceContext<T> ctx) throws Exception {
		throw new UnsupportedOperationException("This method should never be called");
	}

	@Override
	public void cancel() {
		throw new UnsupportedOperationException("This method should never be called");
	}
}

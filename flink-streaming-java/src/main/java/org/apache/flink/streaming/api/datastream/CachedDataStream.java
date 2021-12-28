package org.apache.flink.streaming.api.datastream;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

public class CachedDataStream<T> extends DataStream<T> {
    private final IntermediateDataSetID intermediateDataSetID;

    /**
     * Create a new {@link CachedDataStream} in the given execution environment that wrap the given
     * physical transformation to indicates that the transformation should be cached.
     *
     * @param environment The StreamExecutionEnvironment
     * @param transformation The physical transformation whose intermediate result should be cached.
     */
    public CachedDataStream(
            StreamExecutionEnvironment environment,
            PhysicalTransformation<T> transformation) {
        super(
                environment,
                new CacheTransformation<T>(transformation,
                        String.format("Cache: %s", transformation.getName()),
                        transformation.getOutputType(), transformation.getParallelism()));

        final CacheTransformation<T> t = (CacheTransformation<T>) this.getTransformation();
        intermediateDataSetID = t.getIntermediateDataSetID();
        environment.addCache(intermediateDataSetID, t);
    }

    /**
     * Manually invalidate the cached intermediate result to release the physical resources.
     * Users are not required to invoke this method to release physical resource unless they want
     * to. The cached intermediate result are cleared when The {@link StreamExecutionEnvironment}
     * close.
     *
     * @note After invalidated, the cache may be re-created if this DataStream is used again.
     */
    public void invalidateCache() throws Exception {
        environment.invalidateCache(intermediateDataSetID);
    }
}

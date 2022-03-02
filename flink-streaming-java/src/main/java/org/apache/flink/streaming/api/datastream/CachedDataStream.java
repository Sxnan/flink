/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

/**
 * CacheDataStream represent a DataStream whose input should be cached or has been cached.
 *
 * @param <T> The type of the elements in this stream.
 */
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
            StreamExecutionEnvironment environment, PhysicalTransformation<T> transformation) {
        super(
                environment,
                new CacheTransformation<T>(
                        transformation,
                        String.format("Cache: %s", transformation.getName()),
                        transformation.getOutputType(),
                        transformation.getParallelism()));

        final CacheTransformation<T> t = (CacheTransformation<T>) this.getTransformation();
        intermediateDataSetID = t.getIntermediateDataSetID();
        environment.addCache(intermediateDataSetID, t);
    }

    /**
     * Manually invalidate the cached intermediate result to release the physical resources. Users
     * are not required to invoke this method to release physical resource unless they want to. The
     * cached intermediate result are cleared when The {@link StreamExecutionEnvironment} close.
     *
     * @note After invalidated, the cache may be re-created if this DataStream is used again.
     */
    public void invalidateCache() throws Exception {
        environment.invalidateCache(intermediateDataSetID);
    }
}

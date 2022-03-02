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

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.PersistedIntermediateDataSetDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * A Transformation that indicates the result of the input transformation should be cached or has
 * been cached.
 *
 * @param <T> The type of the elements that result from this {@code Transformation}.
 */
public class CacheTransformation<T> extends Transformation<T> {
    private final PhysicalTransformation<T> transformationToCache;

    private final IntermediateDataSetID intermediateDataSetID;
    private PersistedIntermediateDataSetDescriptor persistedIntermediateDataSetDescriptor;
    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name The name of the {@code Transformation}, this will be shown in Visualizations and
     *     the Log
     * @param outputType The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    public CacheTransformation(
            PhysicalTransformation<T> transformationToCache,
            String name,
            TypeInformation<T> outputType,
            int parallelism) {
        super(name, outputType, parallelism);
        this.transformationToCache = transformationToCache;

        this.intermediateDataSetID = new IntermediateDataSetID();
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        List<Transformation<?>> result = Lists.newArrayList();
        if (persistedIntermediateDataSetDescriptor != null) {
            return result;
        }
        result.add(this);
        result.addAll(transformationToCache.getTransitivePredecessors());
        return result;
    }

    @Override
    public List<Transformation<?>> getInputs() {
        if (persistedIntermediateDataSetDescriptor != null) {
            return Collections.emptyList();
        }
        return Collections.singletonList(transformationToCache);
    }

    public PersistedIntermediateDataSetDescriptor getPersistedIntermediateDataSetDescriptor() {
        return persistedIntermediateDataSetDescriptor;
    }

    public void setPersistedIntermediateDataSetDescriptor(
            PersistedIntermediateDataSetDescriptor persistedIntermediateDataSetDescriptor) {
        this.persistedIntermediateDataSetDescriptor = persistedIntermediateDataSetDescriptor;
    }

    public IntermediateDataSetID getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public PhysicalTransformation<T> getTransformationToCache() {
        return transformationToCache;
    }
}

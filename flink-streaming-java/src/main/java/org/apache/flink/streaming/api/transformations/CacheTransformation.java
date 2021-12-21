package org.apache.flink.streaming.api.transformations;

import org.apache.flink.api.common.PersistedIntermediateDataSetDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

public class CacheTransformation<T> extends Transformation<T> {
    private final PhysicalTransformation<T> transformationToCache;

    private final IntermediateDataSetID intermediateDataSetID;
    private PersistedIntermediateDataSetDescriptor persistedIntermediateDataSetDescriptor;
    /**
     * Creates a new {@code Transformation} with the given name, output type and parallelism.
     *
     * @param name        The name of the {@code Transformation}, this will be shown in Visualizations and
     *                    the Log
     * @param outputType  The output type of this {@code Transformation}
     * @param parallelism The parallelism of this {@code Transformation}
     */
    public CacheTransformation(PhysicalTransformation<T> transformationToCache,
                               String name, TypeInformation<T> outputType, int parallelism) {
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

    public void setPersistedIntermediateDataSetDescriptor(PersistedIntermediateDataSetDescriptor persistedIntermediateDataSetDescriptor) {
        this.persistedIntermediateDataSetDescriptor = persistedIntermediateDataSetDescriptor;
    }

    public IntermediateDataSetID getIntermediateDataSetID() {
        return intermediateDataSetID;
    }

    public PhysicalTransformation<T> getTransformationToCache() {
        return transformationToCache;
    }
}

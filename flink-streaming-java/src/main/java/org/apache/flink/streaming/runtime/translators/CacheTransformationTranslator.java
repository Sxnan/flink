package org.apache.flink.streaming.runtime.translators;

import org.apache.flink.api.common.PersistedIntermediateDataSetDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.graph.SimpleTransformationTranslator;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.TransformationTranslator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class CacheTransformationTranslator<OUT, T extends CacheTransformation<OUT>> extends SimpleTransformationTranslator<OUT, T> {

    @Override
    protected Collection<Integer> translateForBatchInternal(T transformation, Context context) {
        // set the cache properties to the input of the given transformation
        final StreamGraph streamGraph = context.getStreamGraph();
        if (transformation.getPersistedIntermediateDataSetDescriptor() == null) {
            final List<Transformation<?>> inputs = transformation.getInputs();
            Preconditions.checkState(inputs.size() == 1,
                    "There could be only one transformation input to cache");
            Transformation<?> input = inputs.get(0);
            final Collection<Integer> cachedNodeIds = context.getStreamNodeIds(input);

            Preconditions.checkState(cachedNodeIds.size() == 1,
                    "We expect only one stream node for the input transform");

            final Integer cacheNodeId = cachedNodeIds.iterator().next();

            StreamNode cachedStreamNode =
                    streamGraph.getStreamNode(cacheNodeId);

            // create cache
            cachedStreamNode.setShouldCache(true);
            cachedStreamNode.setIntermediateDataSetID(transformation.getIntermediateDataSetID());

            final SimpleOperatorFactory<OUT> operatorFactory = SimpleOperatorFactory.of(new NoOpStreamOperator<>());
            operatorFactory.setChainingStrategy(ChainingStrategy.HEAD);
            streamGraph.addOperator(transformation.getId(),
                    context.getSlotSharingGroup(),
                    transformation.getCoLocationGroupKey(),
                    operatorFactory, transformation.getInputs().get(0).getOutputType(), null,
                    "Cache");
            streamGraph.getStreamNode(transformation.getId()).setConsumeIntermediateDataSetID(transformation.getIntermediateDataSetID());

            streamGraph.setParallelism(transformation.getId(), input.getParallelism());
            streamGraph.setMaxParallelism(transformation.getId(), input.getMaxParallelism());
            streamGraph.addEdge(cacheNodeId, transformation.getId(), 0);
            return Collections.singletonList(cacheNodeId);
        } else {
            // reuse cache
            final PersistedIntermediateDataSetDescriptor idsDescriptor = transformation
                    .getPersistedIntermediateDataSetDescriptor();
            final SimpleOperatorFactory<OUT> operatorFactory = SimpleOperatorFactory.of(new IdentityStreamOperator<>());
            final TypeInformation<OUT> outputType =
                    transformation.getTransformationToCache().getOutputType();
            streamGraph.addLegacySource(transformation.getId(),
                    context.getSlotSharingGroup(),
                    transformation.getCoLocationGroupKey(),
                    operatorFactory, outputType, outputType,
                    "CacheRead");
            streamGraph.setParallelism(transformation.getId(), transformation.getTransformationToCache().getParallelism());
            streamGraph.setMaxParallelism(transformation.getId(), transformation.getTransformationToCache().getMaxParallelism());
            final StreamNode streamNode = streamGraph.getStreamNode(transformation.getId());
            streamNode.setBufferTimeout(-1L);
            streamNode.setCacheIDSDescriptor(idsDescriptor);
            return Collections.singletonList(transformation.getId());
        }
    }

    @Override
    protected Collection<Integer> translateForStreamingInternal(T transformation, Context context) {
        // do nothing
        final List<Transformation<?>> inputs = transformation.getInputs();
        Preconditions.checkState(inputs.size() == 1,
                "There could be only one transformation input to cache");
        Transformation<?> input = inputs.get(0);
        final Collection<Integer> cachedNodeIds = context.getStreamNodeIds(input);

        Preconditions.checkState(cachedNodeIds.size() == 1,
                "We expect only one stream node for the input transform");
        final Integer cachedNodeId = cachedNodeIds.iterator().next();
        return Collections.singletonList(cachedNodeId);
    }

    public static class NoOpStreamOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 4517845269225218313L;

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            // do nothing
        }
    }

    public static class IdentityStreamOperator<T> extends AbstractStreamOperator<T>
            implements OneInputStreamOperator<T, T> {
        private static final long serialVersionUID = 4517845269225218313L;

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
            // do nothing
        }
    }

}
